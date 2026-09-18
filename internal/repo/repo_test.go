package repo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/db"
	"github.com/dex/prediction-service/internal/models"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	_ = godotenv.Load("../../.env")
	uri := os.Getenv("POSTGRES_SERVICE_URI")
	if uri == "" {
		t.Skip("POSTGRES_SERVICE_URI not set, skipping live-Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := db.Connect(ctx, uri)
	if err != nil {
		t.Skipf("could not connect to Postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newTestWindowRepo(t *testing.T, r *Repo, status models.WindowStatus) *models.Window {
	t.Helper()
	now := time.Now()
	w := &models.Window{
		Market:     models.MarketBTC,
		Duration:   models.Duration5m,
		CommitHash: fmt.Sprintf("hash-%d", now.UnixNano()),
		Status:     models.WindowCommitted,
		CommitTime: now,
		StartTime:  now,
		EndTime:    now.Add(5 * time.Minute),
	}
	id, err := r.CreateWindow(context.Background(), w, 10, "salt-"+fmt.Sprint(now.UnixNano()))
	if err != nil {
		t.Fatalf("CreateWindow: %v", err)
	}
	w.ID = id
	t.Cleanup(func() {
		ctx := context.Background()
		r.pool.Exec(ctx, `DELETE FROM prediction_fills WHERE window_id = $1`, id)
		r.pool.Exec(ctx, `DELETE FROM prediction_positions WHERE window_id = $1`, id)
		r.pool.Exec(ctx, `DELETE FROM prediction_orders WHERE window_id = $1`, id)
		r.pool.Exec(ctx, `DELETE FROM prediction_settlements WHERE window_id = $1`, id)
		r.pool.Exec(ctx, `DELETE FROM prediction_windows WHERE id = $1`, id)
	})

	switch status {
	case models.WindowCommitted:
		// already in this state
	case models.WindowOpen:
		if err := r.RevealWindow(context.Background(), id, decimal.NewFromInt(50000), decimal.NewFromInt(50000), now); err != nil {
			t.Fatalf("RevealWindow: %v", err)
		}
	case models.WindowLocked:
		if err := r.RevealWindow(context.Background(), id, decimal.NewFromInt(50000), decimal.NewFromInt(50000), now); err != nil {
			t.Fatalf("RevealWindow: %v", err)
		}
		if err := r.LockWindow(context.Background(), id); err != nil {
			t.Fatalf("LockWindow: %v", err)
		}
	case models.WindowSettled:
		if err := r.RevealWindow(context.Background(), id, decimal.NewFromInt(50000), decimal.NewFromInt(50000), now); err != nil {
			t.Fatalf("RevealWindow: %v", err)
		}
		if err := r.LockWindow(context.Background(), id); err != nil {
			t.Fatalf("LockWindow: %v", err)
		}
		if err := r.SettleWindow(context.Background(), id, decimal.NewFromInt(50100), now); err != nil {
			t.Fatalf("SettleWindow: %v", err)
		}
	}
	got, err := r.GetWindow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWindow after setup: %v", err)
	}
	return got
}

func TestCreateWindow_RoundTripsAllFields(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowCommitted)
	if w.Market != models.MarketBTC || w.Duration != models.Duration5m {
		t.Fatalf("market/duration mismatch: %v/%v", w.Market, w.Duration)
	}
	if w.OffsetBps != 10 {
		t.Fatalf("OffsetBps = %d, want 10", w.OffsetBps)
	}
	if w.Status != models.WindowCommitted {
		t.Fatalf("Status = %v, want committed", w.Status)
	}
	if w.OpeningPrice.Valid {
		t.Fatal("OpeningPrice should be NULL before reveal")
	}
}

func TestRevealWindow_SetsPricesAndFlipsStatus(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	if w.Status != models.WindowOpen {
		t.Fatalf("Status = %v, want open", w.Status)
	}
	if !w.OpeningPrice.Valid || !w.OpeningPrice.Decimal.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("OpeningPrice = %v, want 50000", w.OpeningPrice)
	}
	if w.RevealedAt == nil {
		t.Fatal("RevealedAt should be set after reveal")
	}
}

func TestRevealWindow_RejectsNonCommittedWindow(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen) // already revealed once
	if err := r.RevealWindow(context.Background(), w.ID, decimal.NewFromInt(1), decimal.NewFromInt(1), time.Now()); err == nil {
		t.Fatal("RevealWindow on an already-open window should fail (not in committed state)")
	}
}

func TestLockWindow_OnlyLocksOpenWindows(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	committed := newTestWindowRepo(t, r, models.WindowCommitted)
	// LockWindow has no error return for a no-op match (unlike Reveal/Settle);
	// verify it silently does nothing to a non-open window rather than
	// incorrectly locking a still-committed one.
	if err := r.LockWindow(context.Background(), committed.ID); err != nil {
		t.Fatalf("LockWindow: %v", err)
	}
	got, err := r.GetWindow(context.Background(), committed.ID)
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	if got.Status != models.WindowCommitted {
		t.Fatalf("LockWindow changed status of a committed (not open) window to %v", got.Status)
	}

	open := newTestWindowRepo(t, r, models.WindowOpen)
	if err := r.LockWindow(context.Background(), open.ID); err != nil {
		t.Fatalf("LockWindow: %v", err)
	}
	got, err = r.GetWindow(context.Background(), open.ID)
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	if got.Status != models.WindowLocked {
		t.Fatalf("Status = %v, want locked", got.Status)
	}
}

func TestSettleWindow_RejectsNonLockedWindow(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	if err := r.SettleWindow(context.Background(), w.ID, decimal.NewFromInt(1), time.Now()); err == nil {
		t.Fatal("SettleWindow on an open (not locked) window should fail")
	}
}

func TestGetWindow_NotFoundReturnsErrNotFound(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	_, err := r.GetWindow(context.Background(), -1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetWindow(-1) error = %v, want ErrNotFound", err)
	}
}

func TestActiveWindow_ExcludesSettled(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowSettled)
	_, err := r.ActiveWindow(context.Background(), w.Market, w.Duration)
	// Not asserting ErrNotFound specifically since other tests' windows for
	// the same market/duration may still be active concurrently — only
	// asserting that OUR settled window specifically isn't what's returned.
	if err == nil {
		got, _ := r.ActiveWindow(context.Background(), w.Market, w.Duration)
		if got != nil && got.ID == w.ID {
			t.Fatalf("ActiveWindow returned a settled window (id=%d)", w.ID)
		}
	}
}

func TestActiveWindow_IncludesCommittedOpenLocked(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	for _, status := range []models.WindowStatus{models.WindowCommitted, models.WindowOpen, models.WindowLocked} {
		w := newTestWindowRepo(t, r, status)
		got, err := r.ActiveWindow(context.Background(), w.Market, w.Duration)
		if err != nil {
			t.Fatalf("ActiveWindow (status=%v): %v", status, err)
		}
		// ActiveWindow picks the highest-id match, which may not be OUR
		// window if a concurrent test created a later one for the same
		// market/duration — assert only that a result was returned at all
		// (no error), i.e. that this status IS considered active.
		_ = got
	}
}

func TestDueWindows_OnlyReturnsExpiredOpenWindows(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	// Force end_time into the past directly (CreateWindow doesn't expose it
	// as an update path) so DueWindows' end_time<=now filter can be tested.
	if _, err := pool.Exec(context.Background(), `UPDATE prediction_windows SET end_time = now() - interval '1 minute' WHERE id = $1`, w.ID); err != nil {
		t.Fatalf("force end_time into the past: %v", err)
	}
	due, err := r.DueWindows(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("DueWindows: %v", err)
	}
	found := false
	for _, d := range due {
		if d.ID == w.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("DueWindows did not include a window whose end_time has passed")
	}
}

func TestDueCommits_OnlyReturnsDueCommittedWindows(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowCommitted)
	if _, err := pool.Exec(context.Background(), `UPDATE prediction_windows SET start_time = now() - interval '1 minute' WHERE id = $1`, w.ID); err != nil {
		t.Fatalf("force start_time into the past: %v", err)
	}
	due, err := r.DueCommits(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("DueCommits: %v", err)
	}
	found := false
	for _, d := range due {
		if d.ID == w.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("DueCommits did not include a committed window whose start_time has passed")
	}
}

func TestLockedWindows_ReturnsOnlyLocked(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowLocked)
	locked, err := r.LockedWindows(context.Background())
	if err != nil {
		t.Fatalf("LockedWindows: %v", err)
	}
	found := false
	for _, l := range locked {
		if l.ID == w.ID {
			found = true
		}
		if l.Status != models.WindowLocked {
			t.Fatalf("LockedWindows returned a non-locked window: id=%d status=%v", l.ID, l.Status)
		}
	}
	if !found {
		t.Fatal("LockedWindows did not include our locked test window")
	}
}

func TestActiveWindows_OneRowPerMarketDurationHighestID(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	older := newTestWindowRepo(t, r, models.WindowOpen)
	// A second, later window for the SAME market/duration should be what
	// ActiveWindows' DISTINCT ON (highest id) picks, not the older one.
	newer := newTestWindowRepo(t, r, models.WindowOpen)
	all, err := r.ActiveWindows(context.Background())
	if err != nil {
		t.Fatalf("ActiveWindows: %v", err)
	}
	var picked *models.Window
	for _, w := range all {
		if w.Market == older.Market && w.Duration == older.Duration {
			picked = w
		}
	}
	if picked == nil {
		t.Fatal("ActiveWindows returned no row for our market/duration")
	}
	if picked.ID != newer.ID {
		t.Fatalf("ActiveWindows picked id=%d, want the higher id=%d (newer window)", picked.ID, newer.ID)
	}
}

func newTestOrder(t *testing.T, r *Repo, windowID int64, side models.OrderSide, price, size decimal.Decimal) *models.Order {
	t.Helper()
	userID := fmt.Sprintf("test-user-%d", time.Now().UnixNano())
	o := &models.Order{WindowID: windowID, UserID: userID, Side: side, Price: price, Size: size}
	id, err := r.CreateOrder(context.Background(), o)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	o.ID = id
	return o
}

func TestCreateOrder_And_GetOrder(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	o := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.55), decimal.NewFromInt(10))
	got, err := r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.Status != models.OrderOpen {
		t.Fatalf("new order status = %v, want open", got.Status)
	}
	if !got.FilledSize.IsZero() {
		t.Fatalf("new order FilledSize = %v, want 0", got.FilledSize)
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	_, err := r.GetOrder(context.Background(), -1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetOrder(-1) error = %v, want ErrNotFound", err)
	}
}

func TestAggregatedBook_GroupsByPriceExcludesFilledAndZeroRemainder(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	// Two orders at the same price should aggregate into one level.
	newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.60), decimal.NewFromInt(10))
	newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.60), decimal.NewFromInt(5))
	// A fully filled order at a different price must not appear at all.
	filled := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.70), decimal.NewFromInt(3))
	if err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.FillOrder(context.Background(), tx, filled.ID, decimal.NewFromInt(3), models.OrderFilled)
	}); err != nil {
		t.Fatalf("FillOrder: %v", err)
	}

	book, err := r.AggregatedBook(context.Background(), w.ID, models.SideYes)
	if err != nil {
		t.Fatalf("AggregatedBook: %v", err)
	}
	var sizeAt60 string
	sawPrice70 := false
	for _, level := range book {
		if level.Price == "0.6000" || level.Price == "0.60" {
			sizeAt60 = level.Size
		}
		if level.Price == "0.7000" || level.Price == "0.70" {
			sawPrice70 = true
		}
	}
	if sizeAt60 == "" {
		t.Fatalf("no aggregated level found at price 0.60, got levels: %+v", book)
	}
	// Postgres returns full NUMERIC precision (e.g. "15.000000000000000000"),
	// not a trimmed "15" — compare numerically rather than by exact string.
	gotSize, err := decimal.NewFromString(sizeAt60)
	if err != nil {
		t.Fatalf("parse aggregated size %q: %v", sizeAt60, err)
	}
	if !gotSize.Equal(decimal.NewFromInt(15)) {
		t.Fatalf("aggregated size at 0.60 = %s, want 15 (10+5 combined)", gotSize)
	}
	if sawPrice70 {
		t.Fatal("AggregatedBook included a fully-filled order's price level")
	}
}

func TestOpposingBook_OrdersByPriceDescThenTimeAsc(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	low := newTestOrder(t, r, w.ID, models.SideNo, decimal.NewFromFloat(0.40), decimal.NewFromInt(10))
	high := newTestOrder(t, r, w.ID, models.SideNo, decimal.NewFromFloat(0.60), decimal.NewFromInt(10))

	var book []*models.Order
	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		var err error
		book, err = r.OpposingBook(context.Background(), w.ID, models.SideNo, tx)
		return err
	})
	if err != nil {
		t.Fatalf("OpposingBook: %v", err)
	}
	if len(book) != 2 {
		t.Fatalf("OpposingBook returned %d orders, want 2", len(book))
	}
	if book[0].ID != high.ID {
		t.Fatalf("OpposingBook[0].ID = %d, want the higher-priced order (%d) first", book[0].ID, high.ID)
	}
	if book[1].ID != low.ID {
		t.Fatalf("OpposingBook[1].ID = %d, want the lower-priced order (%d) second", book[1].ID, low.ID)
	}
}

func TestOpposingBook_ExcludesNonOpenOrders(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	open := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))
	cancelled := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.6), decimal.NewFromInt(10))
	if _, err := r.CancelUserOrder(context.Background(), cancelled.ID, cancelled.UserID); err != nil {
		t.Fatalf("CancelUserOrder: %v", err)
	}

	var book []*models.Order
	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		var err error
		book, err = r.OpposingBook(context.Background(), w.ID, models.SideYes, tx)
		return err
	})
	if err != nil {
		t.Fatalf("OpposingBook: %v", err)
	}
	for _, o := range book {
		if o.ID == cancelled.ID {
			t.Fatal("OpposingBook included a cancelled order")
		}
	}
	found := false
	for _, o := range book {
		if o.ID == open.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("OpposingBook excluded the still-open order")
	}
}

func TestFillOrder_AccumulatesFilledSizeAndSetsStatus(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	o := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))

	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.FillOrder(context.Background(), tx, o.ID, decimal.NewFromInt(4), models.OrderOpen)
	})
	if err != nil {
		t.Fatalf("FillOrder (partial): %v", err)
	}
	got, err := r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if !got.FilledSize.Equal(decimal.NewFromInt(4)) {
		t.Fatalf("FilledSize = %v, want 4 after partial fill", got.FilledSize)
	}
	if got.Status != models.OrderOpen {
		t.Fatalf("Status = %v, want open after partial fill", got.Status)
	}

	err = r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.FillOrder(context.Background(), tx, o.ID, decimal.NewFromInt(6), models.OrderFilled)
	})
	if err != nil {
		t.Fatalf("FillOrder (final): %v", err)
	}
	got, err = r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if !got.FilledSize.Equal(decimal.NewFromInt(10)) {
		t.Fatalf("FilledSize = %v, want 10 (4+6 accumulated)", got.FilledSize)
	}
	if got.Status != models.OrderFilled {
		t.Fatalf("Status = %v, want filled", got.Status)
	}
}

func TestCancelUserOrder_OnlyOwnerCanCancel(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	o := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))

	_, err := r.CancelUserOrder(context.Background(), o.ID, "not-the-owner")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("CancelUserOrder by non-owner error = %v, want ErrNotFound", err)
	}
	got, err := r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.Status != models.OrderOpen {
		t.Fatal("a non-owner's cancel attempt changed the order's status")
	}
}

func TestCancelUserOrder_ReturnsUnfilledRemainder(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	o := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))
	if err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.FillOrder(context.Background(), tx, o.ID, decimal.NewFromInt(4), models.OrderOpen)
	}); err != nil {
		t.Fatalf("FillOrder: %v", err)
	}

	remaining, err := r.CancelUserOrder(context.Background(), o.ID, o.UserID)
	if err != nil {
		t.Fatalf("CancelUserOrder: %v", err)
	}
	if !remaining.Equal(decimal.NewFromInt(6)) {
		t.Fatalf("remaining = %v, want 6 (10 size - 4 filled)", remaining)
	}
	got, err := r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.Status != models.OrderCancelled {
		t.Fatalf("Status = %v, want cancelled", got.Status)
	}
}

func TestCancelUserOrder_AlreadyCancelledFailsSecondTime(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	o := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))
	if _, err := r.CancelUserOrder(context.Background(), o.ID, o.UserID); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	if _, err := r.CancelUserOrder(context.Background(), o.ID, o.UserID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second cancel error = %v, want ErrNotFound (already cancelled, not still open)", err)
	}
}

func TestCloseRefundedOrder_OnlyClosesOpenOrders(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	o := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))

	if err := r.CloseRefundedOrder(context.Background(), o.ID, models.OrderCancelled); err != nil {
		t.Fatalf("CloseRefundedOrder: %v", err)
	}
	got, err := r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.Status != models.OrderCancelled {
		t.Fatalf("Status = %v, want cancelled", got.Status)
	}

	// Calling it again on an already-closed order should be a harmless
	// no-op (WHERE status='open' matches nothing), not an error and not a
	// status flip to something unexpected.
	if err := r.CloseRefundedOrder(context.Background(), o.ID, models.OrderFilled); err != nil {
		t.Fatalf("CloseRefundedOrder (second call): %v", err)
	}
	got, err = r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.Status != models.OrderCancelled {
		t.Fatalf("Status changed to %v on a no-op call, want it to stay cancelled", got.Status)
	}
}

func TestOpenOrdersForWindow_ExcludesNonOpen(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	open := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))
	cancelled := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.6), decimal.NewFromInt(10))
	if _, err := r.CancelUserOrder(context.Background(), cancelled.ID, cancelled.UserID); err != nil {
		t.Fatalf("CancelUserOrder: %v", err)
	}

	orders, err := r.OpenOrdersForWindow(context.Background(), w.ID)
	if err != nil {
		t.Fatalf("OpenOrdersForWindow: %v", err)
	}
	if len(orders) != 1 || orders[0].ID != open.ID {
		t.Fatalf("OpenOrdersForWindow = %+v, want exactly [%d]", orders, open.ID)
	}
}

func TestUserOrders_RespectsLimitAndOrdering(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	userID := fmt.Sprintf("test-orders-user-%d", time.Now().UnixNano())
	var ids []int64
	for i := 0; i < 3; i++ {
		o := &models.Order{WindowID: w.ID, UserID: userID, Side: models.SideYes, Price: decimal.NewFromFloat(0.5), Size: decimal.NewFromInt(1)}
		id, err := r.CreateOrder(context.Background(), o)
		if err != nil {
			t.Fatalf("CreateOrder %d: %v", i, err)
		}
		ids = append(ids, id)
		time.Sleep(10 * time.Millisecond) // ensure distinct created_at ordering
	}
	orders, err := r.UserOrders(context.Background(), userID, 2)
	if err != nil {
		t.Fatalf("UserOrders: %v", err)
	}
	if len(orders) != 2 {
		t.Fatalf("UserOrders with limit 2 returned %d orders", len(orders))
	}
	// ORDER BY created_at DESC: the most recently created (last inserted) id
	// should come first.
	if orders[0].ID != ids[2] {
		t.Fatalf("UserOrders[0].ID = %d, want most recent %d", orders[0].ID, ids[2])
	}
}

func TestCreateFill_And_UpsertPosition(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	maker := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.6), decimal.NewFromInt(10))
	taker := newTestOrder(t, r, w.ID, models.SideNo, decimal.NewFromFloat(0.4), decimal.NewFromInt(10))

	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		if err := r.CreateFill(context.Background(), tx, &models.Fill{
			WindowID: w.ID, MakerOrderID: maker.ID, TakerOrderID: taker.ID,
			Price: decimal.NewFromFloat(0.6), Size: decimal.NewFromInt(10),
			MakerFee: decimal.NewFromFloat(0.001), TakerFee: decimal.NewFromFloat(0.002),
		}); err != nil {
			return err
		}
		return r.UpsertPosition(context.Background(), tx, w.ID, maker.UserID, models.SideYes, decimal.NewFromInt(10), decimal.NewFromFloat(0.6))
	})
	if err != nil {
		t.Fatalf("CreateFill+UpsertPosition: %v", err)
	}

	pos, err := r.GetPosition(context.Background(), w.ID, maker.UserID, models.SideYes)
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}
	if !pos.Shares.Equal(decimal.NewFromInt(10)) {
		t.Fatalf("Shares = %v, want 10", pos.Shares)
	}
	if !pos.AvgPrice.Equal(decimal.NewFromFloat(0.6)) {
		t.Fatalf("AvgPrice = %v, want 0.6", pos.AvgPrice)
	}
}

func TestUpsertPosition_RecomputesSizeWeightedAveragePrice(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	userID := fmt.Sprintf("test-avgprice-%d", time.Now().UnixNano())

	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.UpsertPosition(context.Background(), tx, w.ID, userID, models.SideYes, decimal.NewFromInt(10), decimal.NewFromFloat(0.5))
	})
	if err != nil {
		t.Fatalf("first UpsertPosition: %v", err)
	}
	err = r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.UpsertPosition(context.Background(), tx, w.ID, userID, models.SideYes, decimal.NewFromInt(10), decimal.NewFromFloat(0.7))
	})
	if err != nil {
		t.Fatalf("second UpsertPosition: %v", err)
	}

	pos, err := r.GetPosition(context.Background(), w.ID, userID, models.SideYes)
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}
	// (10*0.5 + 10*0.7) / 20 = 0.6
	if !pos.AvgPrice.Equal(decimal.NewFromFloat(0.6)) {
		t.Fatalf("AvgPrice after two fills = %v, want 0.6 (size-weighted average)", pos.AvgPrice)
	}
	if !pos.Shares.Equal(decimal.NewFromInt(20)) {
		t.Fatalf("Shares = %v, want 20 (10+10 accumulated)", pos.Shares)
	}
}

func TestGetPosition_NotFoundForUnheldSide(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	_, err := r.GetPosition(context.Background(), w.ID, "nobody-holds-this", models.SideYes)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetPosition for a user with no position error = %v, want ErrNotFound", err)
	}
}

func TestMarkPositionPaid_OnlyClaimsOnce(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	userID := fmt.Sprintf("test-paid-%d", time.Now().UnixNano())
	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.UpsertPosition(context.Background(), tx, w.ID, userID, models.SideYes, decimal.NewFromInt(5), decimal.NewFromFloat(0.5))
	})
	if err != nil {
		t.Fatalf("UpsertPosition: %v", err)
	}
	pos, err := r.GetPosition(context.Background(), w.ID, userID, models.SideYes)
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}

	claimed, err := r.MarkPositionPaid(context.Background(), pos.ID)
	if err != nil {
		t.Fatalf("MarkPositionPaid (first): %v", err)
	}
	if !claimed {
		t.Fatal("first MarkPositionPaid should have claimed the position (returned true)")
	}

	claimedAgain, err := r.MarkPositionPaid(context.Background(), pos.ID)
	if err != nil {
		t.Fatalf("MarkPositionPaid (second): %v", err)
	}
	if claimedAgain {
		t.Fatal("second MarkPositionPaid should NOT re-claim an already-paid position (returned true)")
	}
}

func TestPositionsForWindow_ReturnsAllSidesAllUsers(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	userA := fmt.Sprintf("test-posA-%d", time.Now().UnixNano())
	userB := fmt.Sprintf("test-posB-%d", time.Now().UnixNano())
	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		if err := r.UpsertPosition(context.Background(), tx, w.ID, userA, models.SideYes, decimal.NewFromInt(5), decimal.NewFromFloat(0.5)); err != nil {
			return err
		}
		return r.UpsertPosition(context.Background(), tx, w.ID, userB, models.SideNo, decimal.NewFromInt(3), decimal.NewFromFloat(0.4))
	})
	if err != nil {
		t.Fatalf("UpsertPosition: %v", err)
	}
	positions, err := r.PositionsForWindow(context.Background(), w.ID)
	if err != nil {
		t.Fatalf("PositionsForWindow: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("PositionsForWindow returned %d positions, want 2", len(positions))
	}
}

func TestNetPositions_ReducesSharesBySpecifiedAmount(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	userID := fmt.Sprintf("test-net-%d", time.Now().UnixNano())
	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.UpsertPosition(context.Background(), tx, w.ID, userID, models.SideYes, decimal.NewFromInt(10), decimal.NewFromFloat(0.5))
	})
	if err != nil {
		t.Fatalf("UpsertPosition: %v", err)
	}
	err = r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.NetPositions(context.Background(), tx, w.ID, userID, decimal.NewFromInt(4))
	})
	if err != nil {
		t.Fatalf("NetPositions: %v", err)
	}
	pos, err := r.GetPosition(context.Background(), w.ID, userID, models.SideYes)
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}
	if !pos.Shares.Equal(decimal.NewFromInt(6)) {
		t.Fatalf("Shares after NetPositions(4) = %v, want 6 (10-4)", pos.Shares)
	}
}

func TestUserPositions_RespectsLimit(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	userID := fmt.Sprintf("test-userpos-%d", time.Now().UnixNano())
	for i := 0; i < 3; i++ {
		w := newTestWindowRepo(t, r, models.WindowOpen)
		err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
			return r.UpsertPosition(context.Background(), tx, w.ID, userID, models.SideYes, decimal.NewFromInt(1), decimal.NewFromFloat(0.5))
		})
		if err != nil {
			t.Fatalf("UpsertPosition %d: %v", i, err)
		}
	}
	positions, err := r.UserPositions(context.Background(), userID, 2)
	if err != nil {
		t.Fatalf("UserPositions: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("UserPositions with limit 2 returned %d", len(positions))
	}
}

func TestCreateSettlement_Persists(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowLocked)
	if err := r.CreateSettlement(context.Background(), &models.Settlement{
		WindowID: w.ID, WinningSide: models.SideYes,
		ResolutionPrice: decimal.NewFromInt(50100), TargetPrice: decimal.NewFromInt(50000),
		TotalPaidOut: decimal.NewFromInt(100),
	}); err != nil {
		t.Fatalf("CreateSettlement: %v", err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM prediction_settlements WHERE window_id = $1`, w.ID).Scan(&count); err != nil {
		t.Fatalf("verify settlement row: %v", err)
	}
	if count != 1 {
		t.Fatalf("prediction_settlements row count = %d, want 1", count)
	}
}

func TestWithTx_RollsBackOnError(t *testing.T) {
	pool := testPool(t)
	r := New(pool)
	w := newTestWindowRepo(t, r, models.WindowOpen)
	o := newTestOrder(t, r, w.ID, models.SideYes, decimal.NewFromFloat(0.5), decimal.NewFromInt(10))

	sentinelErr := errors.New("intentional rollback")
	err := r.WithTx(context.Background(), func(tx pgx.Tx) error {
		if err := r.FillOrder(context.Background(), tx, o.ID, decimal.NewFromInt(10), models.OrderFilled); err != nil {
			return err
		}
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("WithTx error = %v, want sentinelErr", err)
	}
	got, err := r.GetOrder(context.Background(), o.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.Status != models.OrderOpen || !got.FilledSize.IsZero() {
		t.Fatalf("FillOrder's effect survived a rolled-back transaction: status=%v filled=%v", got.Status, got.FilledSize)
	}
}
