// Package repo is the data-access layer over this service's own
// prediction_* Postgres tables.
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/models"
)

var ErrNotFound = errors.New("not found")

type Repo struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

// --- windows ---

// CreateWindow persists offsetBps/salt immediately, alongside the commit
// hash, so a service restart between commit and reveal never loses the
// random target — the commit-reveal fairness guarantee only requires that
// offsetBps/salt aren't PUBLISHED before reveal (enforced at the API read
// layer, see api.handleActiveWindows), not that they're withheld from this
// service's own database.
func (r *Repo) CreateWindow(ctx context.Context, w *models.Window, offsetBps int64, salt string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, `
		INSERT INTO prediction_windows
			(market, duration, commit_hash, offset_bps, salt, status, commit_time, start_time, end_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`, w.Market, w.Duration, w.CommitHash, offsetBps, salt, w.Status, w.CommitTime, w.StartTime, w.EndTime).Scan(&id)
	return id, err
}

// RevealWindow fills in the opening/target price once a committed window's
// trading actually opens, flipping it to WindowOpen. offsetBps/salt were
// already stored at commit time (see CreateWindow); this only computes and
// stores the price-dependent target now that the live opening price is known.
func (r *Repo) RevealWindow(ctx context.Context, id int64, openingPrice, targetPrice decimal.Decimal, now time.Time) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE prediction_windows
		SET opening_price = $2, target_price = $3,
		    status = 'open', revealed_at = $4
		WHERE id = $1 AND status = 'committed'
	`, id, openingPrice, targetPrice, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("window %d not in committed state", id)
	}
	return nil
}

func (r *Repo) LockWindow(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx, `UPDATE prediction_windows SET status = 'locked' WHERE id = $1 AND status = 'open'`, id)
	return err
}

func (r *Repo) SettleWindow(ctx context.Context, id int64, resolutionPrice decimal.Decimal, now time.Time) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE prediction_windows
		SET resolution_price = $2, status = 'settled', settled_at = $3
		WHERE id = $1 AND status = 'locked'
	`, id, resolutionPrice, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("window %d not in locked state", id)
	}
	return nil
}

func scanWindow(row pgx.Row) (*models.Window, error) {
	w := &models.Window{}
	err := row.Scan(
		&w.ID, &w.Market, &w.Duration, &w.CommitHash, &w.Salt, &w.OffsetBps,
		&w.OpeningPrice, &w.TargetPrice, &w.ResolutionPrice, &w.Status,
		&w.CommitTime, &w.StartTime, &w.EndTime, &w.RevealedAt, &w.SettledAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return w, nil
}

const windowCols = `id, market, duration, commit_hash, salt, offset_bps,
	opening_price, target_price, resolution_price, status,
	commit_time, start_time, end_time, revealed_at, settled_at`

func (r *Repo) GetWindow(ctx context.Context, id int64) (*models.Window, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+windowCols+` FROM prediction_windows WHERE id = $1`, id)
	return scanWindow(row)
}

// LatestWindow returns the most recently created window for a market/duration
// pair, regardless of status — used by the Round Manager to find what to
// roll forward from.
func (r *Repo) LatestWindow(ctx context.Context, market models.Market, duration models.Duration) (*models.Window, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT `+windowCols+` FROM prediction_windows
		WHERE market = $1 AND duration = $2
		ORDER BY id DESC LIMIT 1
	`, market, duration)
	return scanWindow(row)
}

// ActiveWindow returns the current open-or-committed window for a
// market/duration pair, for order placement and UI display.
func (r *Repo) ActiveWindow(ctx context.Context, market models.Market, duration models.Duration) (*models.Window, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT `+windowCols+` FROM prediction_windows
		WHERE market = $1 AND duration = $2 AND status IN ('committed', 'open', 'locked')
		ORDER BY id DESC LIMIT 1
	`, market, duration)
	return scanWindow(row)
}

// ActiveWindows returns the current active window for every market/duration
// pair in one query, instead of one round-trip per pair (6 sequential
// round-trips to a remote Postgres instance was the actual cause of
// GET /prediction/windows taking anywhere from 0.7s to 5+s — any one of six
// serialized network calls having a slow moment stalled the whole request).
// DISTINCT ON (market, duration) with this ordering picks the same
// highest-id row per pair that six separate ActiveWindow calls would have.
func (r *Repo) ActiveWindows(ctx context.Context) ([]*models.Window, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (market, duration) `+windowCols+`
		FROM prediction_windows
		WHERE status IN ('committed', 'open', 'locked')
		ORDER BY market, duration, id DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Window
	for rows.Next() {
		w, err := scanWindow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// DueWindows returns open windows whose end_time has passed, for the Round
// Manager to lock.
func (r *Repo) DueWindows(ctx context.Context, now time.Time) ([]*models.Window, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+windowCols+` FROM prediction_windows
		WHERE status = 'open' AND end_time <= $1
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Window
	for rows.Next() {
		w, err := scanWindow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// LockedWindows returns windows awaiting settlement.
func (r *Repo) LockedWindows(ctx context.Context) ([]*models.Window, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+windowCols+` FROM prediction_windows WHERE status = 'locked'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Window
	for rows.Next() {
		w, err := scanWindow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// DueCommits returns committed windows whose start_time has arrived, for the
// Round Manager to reveal and open.
func (r *Repo) DueCommits(ctx context.Context, now time.Time) ([]*models.Window, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+windowCols+` FROM prediction_windows
		WHERE status = 'committed' AND start_time <= $1
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Window
	for rows.Next() {
		w, err := scanWindow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// --- orders ---

func (r *Repo) CreateOrder(ctx context.Context, o *models.Order) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, `
		INSERT INTO prediction_orders (window_id, user_id, side, price, size)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, o.WindowID, o.UserID, o.Side, o.Price, o.Size).Scan(&id)
	return id, err
}

func scanOrder(row pgx.Row) (*models.Order, error) {
	o := &models.Order{}
	err := row.Scan(&o.ID, &o.WindowID, &o.UserID, &o.Side, &o.Price, &o.Size, &o.FilledSize, &o.Status, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return o, nil
}

const orderCols = `id, window_id, user_id, side, price, size, filled_size, status, created_at, updated_at`

func (r *Repo) GetOrder(ctx context.Context, id int64) (*models.Order, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+orderCols+` FROM prediction_orders WHERE id = $1`, id)
	return scanOrder(row)
}

// BookLevel is one aggregated price level of a window's public order book —
// individual orders/users are never exposed here, only price+total size.
type BookLevel struct {
	Price string
	Size  string
}

// AggregatedBook returns the public order book for one side of a window,
// aggregated by price (best price first) with per-user identity stripped —
// safe to expose to any client, unlike OpposingBook's per-order view used
// internally by the matcher.
func (r *Repo) AggregatedBook(ctx context.Context, windowID int64, side models.OrderSide) ([]BookLevel, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT price, SUM(size - filled_size) AS remaining
		FROM prediction_orders
		WHERE window_id = $1 AND side = $2 AND status = 'open'
		GROUP BY price
		HAVING SUM(size - filled_size) > 0
		ORDER BY price DESC
	`, windowID, side)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BookLevel
	for rows.Next() {
		var level BookLevel
		if err := rows.Scan(&level.Price, &level.Size); err != nil {
			return nil, err
		}
		out = append(out, level)
	}
	return out, rows.Err()
}

// OpposingBook returns resting (open, unfilled) orders on the opposite side
// of a window's book, best price first (highest price for the side being
// bought against, i.e. price-time priority), for matching an incoming order.
//
// A NO order at price p matches a YES order at price (1-p): both express
// the same implied probability from opposite sides, so callers pass the
// complementary price to match against.
func (r *Repo) OpposingBook(ctx context.Context, windowID int64, side models.OrderSide, tx pgx.Tx) ([]*models.Order, error) {
	rows, err := tx.Query(ctx, `
		SELECT `+orderCols+` FROM prediction_orders
		WHERE window_id = $1 AND side = $2 AND status = 'open'
		ORDER BY price DESC, created_at ASC
		FOR UPDATE
	`, windowID, side)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (r *Repo) FillOrder(ctx context.Context, tx pgx.Tx, id int64, addFilled decimal.Decimal, newStatus models.OrderStatus) error {
	_, err := tx.Exec(ctx, `
		UPDATE prediction_orders
		SET filled_size = filled_size + $2, status = $3, updated_at = now()
		WHERE id = $1
	`, id, addFilled, newStatus)
	return err
}

// CancelUserOrder cancels a still-open order owned by userID, returning the
// unfilled remainder that must be unlocked.
func (r *Repo) CancelUserOrder(ctx context.Context, orderID int64, userID string) (decimal.Decimal, error) {
	var remaining decimal.Decimal
	err := r.pool.QueryRow(ctx, `
		UPDATE prediction_orders
		SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND user_id = $2 AND status = 'open'
		RETURNING size - filled_size
	`, orderID, userID).Scan(&remaining)
	if errors.Is(err, pgx.ErrNoRows) {
		return decimal.Zero, ErrNotFound
	}
	return remaining, err
}

// CloseRefundedOrder transitions an order out of "open" once its unfilled
// remainder has been refunded at round lock — it never fills further, so
// leaving it "open" would misrepresent it as still live to anything reading
// order status later. newStatus is "cancelled" for a never-filled order or
// "filled" for a partially-filled one whose remainder just got refunded
// (its filled portion still settles normally; there's just no remainder
// left to ever match).
func (r *Repo) CloseRefundedOrder(ctx context.Context, orderID int64, newStatus models.OrderStatus) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE prediction_orders SET status = $2, updated_at = now()
		WHERE id = $1 AND status = 'open'
	`, orderID, newStatus)
	return err
}

// OpenOrdersForWindow returns all still-open orders in a window, used at
// lock time to refund unfilled remainders.
func (r *Repo) OpenOrdersForWindow(ctx context.Context, windowID int64) ([]*models.Order, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+orderCols+` FROM prediction_orders WHERE window_id = $1 AND status = 'open'`, windowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (r *Repo) UserOrders(ctx context.Context, userID string, limit int) ([]*models.Order, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+orderCols+` FROM prediction_orders WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2
	`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// --- fills ---

func (r *Repo) CreateFill(ctx context.Context, tx pgx.Tx, f *models.Fill) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO prediction_fills (window_id, maker_order_id, taker_order_id, price, size, maker_fee, taker_fee)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, f.WindowID, f.MakerOrderID, f.TakerOrderID, f.Price, f.Size, f.MakerFee, f.TakerFee)
	return err
}

// --- positions ---

// UpsertPosition adds shares/notional to a user's position for a window+side,
// recomputing the size-weighted average price, inside an existing tx.
func (r *Repo) UpsertPosition(ctx context.Context, tx pgx.Tx, windowID int64, userID string, side models.OrderSide, addShares, price decimal.Decimal) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO prediction_positions (window_id, user_id, side, shares, avg_price)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (window_id, user_id, side) DO UPDATE SET
			avg_price = (prediction_positions.shares * prediction_positions.avg_price + $4 * $5) / (prediction_positions.shares + $4),
			shares = prediction_positions.shares + $4,
			updated_at = now()
	`, windowID, userID, side, addShares, price)
	return err
}

func scanPosition(row pgx.Row) (*models.Position, error) {
	p := &models.Position{}
	err := row.Scan(&p.ID, &p.WindowID, &p.UserID, &p.Side, &p.Shares, &p.AvgPrice, &p.Realized, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return p, nil
}

const positionCols = `id, window_id, user_id, side, shares, avg_price, realized, created_at, updated_at`

// PositionsForWindow returns all positions in a window, for settlement payout.
func (r *Repo) PositionsForWindow(ctx context.Context, windowID int64) ([]*models.Position, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+positionCols+` FROM prediction_positions WHERE window_id = $1`, windowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Position
	for rows.Next() {
		p, err := scanPosition(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GetPosition returns a user's position for one window+side, or ErrNotFound
// if they hold none.
func (r *Repo) GetPosition(ctx context.Context, windowID int64, userID string, side models.OrderSide) (*models.Position, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+positionCols+` FROM prediction_positions WHERE window_id = $1 AND user_id = $2 AND side = $3`, windowID, userID, side)
	return scanPosition(row)
}

// NetPositions reduces a user's opposing YES and NO holdings in a window by
// min(yesShares, noShares) — the standard binary-market equivalence that
// holding equal YES and NO shares is a fully hedged, closed position worth a
// fixed $1-per-share regardless of outcome. This is how "selling" a position
// is implemented: closing N shares of YES is done by acquiring N shares of
// NO (see Matcher.Sell), then netting both down here instead of settling the
// hedge at resolution. realizedDelta is added to both sides' Realized
// column for bookkeeping/audit only; the actual cash movement happens via
// backendclient.Credit in the caller, inside the same transaction.
func (r *Repo) NetPositions(ctx context.Context, tx pgx.Tx, windowID int64, userID string, netShares decimal.Decimal) error {
	_, err := tx.Exec(ctx, `
		UPDATE prediction_positions
		SET shares = shares - $3, updated_at = now()
		WHERE window_id = $1 AND user_id = $2 AND side IN ('yes', 'no')
	`, windowID, userID, netShares)
	return err
}

func (r *Repo) UserPositions(ctx context.Context, userID string, limit int) ([]*models.Position, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+positionCols+` FROM prediction_positions WHERE user_id = $1 ORDER BY updated_at DESC LIMIT $2
	`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Position
	for rows.Next() {
		p, err := scanPosition(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// --- settlements ---

func (r *Repo) CreateSettlement(ctx context.Context, s *models.Settlement) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO prediction_settlements (window_id, winning_side, resolution_price, target_price, total_paid_out)
		VALUES ($1, $2, $3, $4, $5)
	`, s.WindowID, s.WinningSide, s.ResolutionPrice, s.TargetPrice, s.TotalPaidOut)
	return err
}

// WithTx runs fn inside a transaction.
func (r *Repo) WithTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
