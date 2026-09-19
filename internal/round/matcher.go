package round

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/backendclient"
	"github.com/dex/prediction-service/internal/models"
	"github.com/dex/prediction-service/internal/repo"
)

// Fee rates per the plan: resting (maker) orders pay less than the
// order that crosses the book and takes liquidity (taker).
var (
	makerFeeRate = decimal.NewFromFloat(0.00015) // 0.015%
	takerFeeRate = decimal.NewFromFloat(0.00045) // 0.045%
)

// Matcher runs price-time-priority matching for one window's YES/NO book.
// There is no market maker — every fill is between two real user orders, so
// an order can rest partially or fully unfilled if there's no opposing
// liquidity at an acceptable price (see the plan's unfilled-order risk
// callout; unfilled remainders are refunded at window lock, see round.go).
type Matcher struct {
	repo   *repo.Repo
	client *backendclient.Client
}

func NewMatcher(r *repo.Repo, c *backendclient.Client) *Matcher {
	return &Matcher{repo: r, client: c}
}

// opposite returns the other side of a binary market.
func opposite(s models.OrderSide) models.OrderSide {
	if s == models.SideYes {
		return models.SideNo
	}
	return models.SideYes
}

// PlaceOrder locks the order's notional cost against the user's real balance,
// then matches it price-time-priority against the resting opposite-side book,
// crediting/debiting real balances and applying maker/taker fees per fill.
// Any unfilled remainder rests on the book as an open order.
//
// A YES order at price p and a NO order at price q are compatible when
// p + q >= 1 (i.e. q >= 1-p): the resting order's price is what the trade
// executes at, giving price-time priority to whoever was there first.
func (m *Matcher) PlaceOrder(ctx context.Context, o *models.Order) (int64, []*models.Fill, error) {
	if err := m.client.EnsureUser(ctx, o.UserID); err != nil {
		return 0, nil, fmt.Errorf("ensure user: %w", err)
	}

	cost := o.Price.Mul(o.Size)
	if o.Side == models.SideNo {
		cost = decimal.NewFromInt(1).Sub(o.Price).Mul(o.Size)
	}
	rawCost := backendclient.ToRawUnits(cost)
	if err := m.client.Lock(ctx, o.UserID, "BI2XUSD", rawCost); err != nil {
		return 0, nil, fmt.Errorf("lock order cost: %w", err)
	}

	orderID, err := m.repo.CreateOrder(ctx, o)
	if err != nil {
		_ = m.client.Unlock(ctx, o.UserID, "BI2XUSD", rawCost)
		return 0, nil, fmt.Errorf("create order: %w", err)
	}
	o.ID = orderID

	// Matching plan is decided, every order's filled_size/status is updated,
	// and a durable pending-fill row is written for each match — all in one
	// short transaction. This is the only step that needs the resting book's
	// FOR UPDATE lock. The lock used to be held across the settleFill HTTP
	// calls below (M7/PRED-H2: pinned a DB connection and the lock rows for
	// the full network round-trip of every fill, so a slow or hung backend
	// call could exhaust the pool and block all other matching on the
	// window). Splitting the transaction here means the lock is only held
	// for local Postgres work; HTTP settlement happens with no transaction
	// open at all. The pending-fill row is what makes that safe: if the
	// process dies or a backend call fails after this transaction commits,
	// the match isn't lost — it's sitting in prediction_pending_fills for
	// the reconciliation sweep (round/reconcile.go) to retry.
	var pending []*models.PendingFill
	err = m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		book, err := m.repo.OpposingBook(ctx, o.WindowID, opposite(o.Side), tx)
		if err != nil {
			return err
		}
		remaining := o.Size
		for _, resting := range book {
			if remaining.IsZero() {
				break
			}
			restingRemaining := resting.Size.Sub(resting.FilledSize)
			if restingRemaining.LessThanOrEqual(decimal.Zero) {
				continue
			}

			// Compatibility check: incoming YES@p vs resting NO@q needs p+q>=1;
			// incoming NO@p vs resting YES@q needs p+q>=1 too (symmetric).
			var sumOK bool
			if o.Side == models.SideYes {
				sumOK = o.Price.Add(resting.Price).GreaterThanOrEqual(decimal.NewFromInt(1))
			} else {
				sumOK = resting.Price.Add(o.Price).GreaterThanOrEqual(decimal.NewFromInt(1))
			}
			if !sumOK {
				continue
			}

			fillSize := decimal.Min(remaining, restingRemaining)
			// Trade executes at the resting order's price (maker sets price).
			execPrice := resting.Price

			pf := &models.PendingFill{
				WindowID: o.WindowID, MakerOrderID: resting.ID, TakerOrderID: o.ID,
				MakerUserID: resting.UserID, TakerUserID: o.UserID,
				MakerSide: resting.Side, TakerSide: o.Side,
				ExecPrice: execPrice, Size: fillSize,
				IdempotencyKey: uuid.NewString(),
			}
			pfID, err := m.repo.CreatePendingFill(ctx, tx, pf)
			if err != nil {
				return err
			}
			pf.ID = pfID
			pending = append(pending, pf)

			remaining = remaining.Sub(fillSize)

			restingNewFilled := resting.FilledSize.Add(fillSize)
			restingStatus := models.OrderOpen
			if restingNewFilled.GreaterThanOrEqual(resting.Size) {
				restingStatus = models.OrderFilled
			}
			if err := m.repo.FillOrder(ctx, tx, resting.ID, fillSize, restingStatus); err != nil {
				return err
			}
			resting.FilledSize = restingNewFilled
		}

		takerNewFilled := o.Size.Sub(remaining)
		takerStatus := models.OrderOpen
		if remaining.IsZero() {
			takerStatus = models.OrderFilled
		}
		if err := m.repo.FillOrder(ctx, tx, o.ID, takerNewFilled, takerStatus); err != nil {
			return err
		}
		o.FilledSize = takerNewFilled
		o.Status = takerStatus
		return nil
	})
	if err != nil {
		return orderID, nil, fmt.Errorf("match order: %w", err)
	}

	// Settlement (real balance movement over HTTP, plus the position/fill
	// bookkeeping that only matters once it succeeds) runs with no
	// transaction open. Order status above is already durably committed, so
	// a failure here just leaves that pending-fill row unresolved for the
	// reconciliation sweep to pick up and retry later, instead of silently
	// losing the fill.
	var fills []*models.Fill
	for _, pf := range pending {
		if err := m.settleFillByPlan(ctx, pf); err != nil {
			return orderID, fills, fmt.Errorf("settle fill: %w", err)
		}
		fills = append(fills, &models.Fill{
			WindowID:     pf.WindowID,
			MakerOrderID: pf.MakerOrderID,
			TakerOrderID: pf.TakerOrderID,
			Price:        pf.ExecPrice,
			Size:         pf.Size,
		})
	}
	return orderID, fills, nil
}

// settleFillByPlan moves real BI2XUSD for one already-durably-recorded
// match: consumes both sides' locked funds via signed Credit calls, records
// fee revenue via SettleFee, updates both users' positions, and — only once
// all of that succeeds — deletes the pending-fill row. execPrice is the
// YES-probability price the resting (maker) order set; the taker's
// economics are computed from the complementary side. Runs with no DB
// transaction open around the HTTP calls; UpsertPosition/CreateFill commit
// in their own short transaction since order-level state is already final
// by this point. Called both from PlaceOrder (immediately after a match)
// and from the reconciliation sweep (retrying a pending fill left over from
// a crash or a prior failed attempt) — the pending-fill row's
// idempotency key is what makes a sweep retry of an already-partially-
// applied HTTP call safe to send again.
func (m *Matcher) settleFillByPlan(ctx context.Context, pf *models.PendingFill) error {
	execPrice, size := pf.ExecPrice, pf.Size

	// Cost each side actually pays for this fill, in their own side's terms.
	makerCost := execPrice.Mul(size)
	takerCost := decimal.NewFromInt(1).Sub(execPrice).Mul(size)
	if pf.MakerSide == models.SideNo {
		makerCost, takerCost = takerCost, makerCost
	}

	makerFee := makerCost.Mul(makerFeeRate)
	takerFee := takerCost.Mul(takerFeeRate)

	// Consume the locked hold: the lock was taken at order-placement price,
	// which may differ slightly from execPrice for a maker whose resting
	// price improved after being crossed; both sides only ever pay execPrice
	// economics here, debited via a negative Credit against their lock.
	// Each call carries pf.IdempotencyKey (plus a role suffix, since one
	// pending fill needs 4 distinct backend calls) so a sweep retry can be
	// told apart from the original attempt at the same logical operation.
	if err := m.client.CreditIdempotent(ctx, pf.MakerUserID, "BI2XUSD", "-"+backendclient.ToRawUnits(makerCost.Add(makerFee)), pf.IdempotencyKey+":maker-debit"); err != nil {
		return fmt.Errorf("debit maker: %w", err)
	}
	if err := m.client.CreditIdempotent(ctx, pf.TakerUserID, "BI2XUSD", "-"+backendclient.ToRawUnits(takerCost.Add(takerFee)), pf.IdempotencyKey+":taker-debit"); err != nil {
		return fmt.Errorf("debit taker: %w", err)
	}
	if err := m.client.SettleFeeIdempotent(ctx, pf.MakerUserID, "BI2XUSD", backendclient.ToRawUnits(makerFee), pf.IdempotencyKey+":maker-fee"); err != nil {
		return fmt.Errorf("settle maker fee: %w", err)
	}
	if err := m.client.SettleFeeIdempotent(ctx, pf.TakerUserID, "BI2XUSD", backendclient.ToRawUnits(takerFee), pf.IdempotencyKey+":taker-fee"); err != nil {
		return fmt.Errorf("settle taker fee: %w", err)
	}

	takerPrice := execPrice
	if pf.TakerSide == models.SideNo {
		takerPrice = decimal.NewFromInt(1).Sub(execPrice)
	}
	if err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		if err := m.repo.UpsertPosition(ctx, tx, pf.WindowID, pf.MakerUserID, pf.MakerSide, size, execPrice); err != nil {
			return err
		}
		if err := m.repo.UpsertPosition(ctx, tx, pf.WindowID, pf.TakerUserID, pf.TakerSide, size, takerPrice); err != nil {
			return err
		}
		return m.repo.CreateFill(ctx, tx, &models.Fill{
			WindowID: pf.WindowID, MakerOrderID: pf.MakerOrderID, TakerOrderID: pf.TakerOrderID,
			Price: execPrice, Size: size, MakerFee: makerFee, TakerFee: takerFee,
		})
	}); err != nil {
		return err
	}
	return m.repo.DeletePendingFill(ctx, pf.ID)
}

// Sell closes up to size shares of a user's existing side position before
// resolution. There is no separate "ask" order type — selling N shares of
// YES is economically identical to buying N shares of NO (holding equal
// YES and NO is a fully hedged position worth exactly $1/share regardless
// of outcome), so this places a real order on the opposite side, matches it
// through the normal book, then nets down both sides by however much
// actually filled. Any unfilled remainder simply rests on the book as a
// normal opposite-side order — the user still holds their original
// position for that portion, unhedged, exactly as if they'd placed that
// order directly.
func (m *Matcher) Sell(ctx context.Context, windowID int64, userID string, side models.OrderSide, size, limitPrice decimal.Decimal) (int64, decimal.Decimal, error) {
	position, err := m.repo.GetPosition(ctx, windowID, userID, side)
	if err != nil {
		return 0, decimal.Zero, fmt.Errorf("no position to sell: %w", err)
	}
	if size.GreaterThan(position.Shares) {
		return 0, decimal.Zero, fmt.Errorf("cannot sell %s shares, only %s held", size, position.Shares)
	}

	hedgeSide := opposite(side)
	hedgePrice := limitPrice
	if side == models.SideYes {
		hedgePrice = decimal.NewFromInt(1).Sub(limitPrice)
	}

	hedgeOrder := &models.Order{WindowID: windowID, UserID: userID, Side: hedgeSide, Price: hedgePrice, Size: size}
	orderID, _, err := m.PlaceOrder(ctx, hedgeOrder)
	if err != nil {
		return 0, decimal.Zero, err
	}

	if hedgeOrder.FilledSize.GreaterThan(decimal.Zero) {
		if err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
			return m.repo.NetPositions(ctx, tx, windowID, userID, hedgeOrder.FilledSize)
		}); err != nil {
			return orderID, decimal.Zero, fmt.Errorf("net positions after sell: %w", err)
		}
	}
	return orderID, hedgeOrder.FilledSize, nil
}

// CancelOrder cancels a resting order and unlocks its unfilled remainder.
func (m *Matcher) CancelOrder(ctx context.Context, orderID int64, userID string, side models.OrderSide, price decimal.Decimal) error {
	remaining, err := m.repo.CancelUserOrder(ctx, orderID, userID)
	if err != nil {
		return err
	}
	refund := price.Mul(remaining)
	if side == models.SideNo {
		refund = decimal.NewFromInt(1).Sub(price).Mul(remaining)
	}
	return m.client.Unlock(ctx, userID, "BI2XUSD", backendclient.ToRawUnits(refund))
}
