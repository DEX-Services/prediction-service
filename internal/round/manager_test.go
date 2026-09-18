package round

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/backendclient"
	"github.com/dex/prediction-service/internal/history"
	"github.com/dex/prediction-service/internal/index"
	"github.com/dex/prediction-service/internal/models"
	"github.com/dex/prediction-service/internal/repo"
	"github.com/dex/prediction-service/internal/roundcache"
)

// testManagerDeps builds a Manager against the real Postgres/Redis instances
// this service shares with the rest of the platform, using a distinctive
// test-only price-key prefix so it never reads or writes the real live price
// feed's keys. Skips gracefully if either isn't configured.
func testManagerDeps(t *testing.T) (*Manager, *repo.Repo, *redis.Client, *fakeBackend, *pgxpool.Pool) {
	t.Helper()
	pool := testPool(t)
	redisURI := os.Getenv("REDIS_SERVICE_URI")
	if redisURI == "" {
		t.Skip("REDIS_SERVICE_URI not set, skipping live-Redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prices, err := index.New(ctx, redisURI, "test-manager-price", 10*time.Second)
	if err != nil {
		t.Skipf("could not connect index.Reader to Redis: %v", err)
	}
	t.Cleanup(func() { prices.Close() })

	cache, err := roundcache.New(ctx, redisURI)
	if err != nil {
		t.Skipf("could not connect roundcache.Cache to Redis: %v", err)
	}
	t.Cleanup(func() { cache.Close() })

	hist, err := history.New(ctx, redisURI)
	if err != nil {
		t.Skipf("could not connect history.Store to Redis: %v", err)
	}
	t.Cleanup(func() { hist.Close() })

	r := repo.New(pool)
	fb := newFakeBackend()
	srv := fb.server(t)
	client := backendclient.NewForTest(srv.URL, "test-secret", srv.Client())
	matcher := NewMatcher(r, client)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	m := NewManager(r, prices, matcher, client, hist, cache, log, nil)

	opts, err := redis.ParseURL(redisURI)
	if err != nil {
		t.Fatalf("parse redis URI for direct test writes: %v", err)
	}
	raw := redis.NewClient(opts)
	t.Cleanup(func() { raw.Close() })

	return m, r, raw, fb, pool
}

// writeTestPrice publishes a price under the test-only prefix
// testManagerDeps' index.Reader is configured with, so tests control
// freshness/value exactly without touching the real live price feed.
func writeTestPrice(t *testing.T, raw *redis.Client, asset string, last float64, timestampMs int64) {
	t.Helper()
	data, err := json.Marshal(struct {
		Last        float64 `json:"last"`
		TimestampMs int64   `json:"timestamp_ms"`
	}{Last: last, TimestampMs: timestampMs})
	if err != nil {
		t.Fatalf("marshal test price: %v", err)
	}
	key := "test-manager-price:" + asset
	if err := raw.Set(context.Background(), key, data, time.Minute).Err(); err != nil {
		t.Fatalf("write test price: %v", err)
	}
	t.Cleanup(func() { raw.Del(context.Background(), key) })
}

func TestManager_CommitNextWindow_CreatesCommittedWindow(t *testing.T) {
	m, r, _, _, _ := testManagerDeps(t)
	startTime := time.Now()
	if err := m.commitNextWindow(context.Background(), models.MarketBTC, models.Duration5m, startTime); err != nil {
		t.Fatalf("commitNextWindow: %v", err)
	}
	w, err := r.ActiveWindow(context.Background(), models.MarketBTC, models.Duration5m)
	if err != nil {
		t.Fatalf("ActiveWindow: %v", err)
	}
	if w.Status != models.WindowCommitted {
		t.Fatalf("Status = %v, want committed", w.Status)
	}
	if w.CommitHash == "" {
		t.Fatal("CommitHash is empty — NewCommitment's hash was not persisted")
	}
	if !w.EndTime.Equal(startTime.Add(5 * time.Minute)) {
		t.Fatalf("EndTime = %v, want startTime + 5m", w.EndTime)
	}
}

func TestManager_OpenDueCommits_RevealsWithFreshPrice(t *testing.T) {
	m, r, raw, _, _ := testManagerDeps(t)
	now := time.Now()
	writeTestPrice(t, raw, "BTC", 60000, now.UnixMilli())

	if err := m.commitNextWindow(context.Background(), models.MarketBTC, models.Duration5m, now.Add(-time.Minute)); err != nil {
		t.Fatalf("commitNextWindow: %v", err)
	}
	if err := m.openDueCommits(context.Background(), now); err != nil {
		t.Fatalf("openDueCommits: %v", err)
	}

	w, err := r.ActiveWindow(context.Background(), models.MarketBTC, models.Duration5m)
	if err != nil {
		t.Fatalf("ActiveWindow: %v", err)
	}
	if w.Status != models.WindowOpen {
		t.Fatalf("Status = %v, want open after openDueCommits with a fresh price", w.Status)
	}
	if !w.OpeningPrice.Valid || !w.OpeningPrice.Decimal.Equal(decimal.NewFromInt(60000)) {
		t.Fatalf("OpeningPrice = %v, want 60000", w.OpeningPrice)
	}
}

func TestManager_OpenDueCommits_WaitsWithinGracePeriodOnStalePrice(t *testing.T) {
	m, r, _, _, _ := testManagerDeps(t)
	now := time.Now()
	// No price written at all for this market — openDueCommits must NOT
	// reveal yet since we're still within revealGracePeriod (5s) of
	// commitTime (which is "now" for a freshly committed window).
	if err := m.commitNextWindow(context.Background(), models.MarketETH, models.Duration5m, now.Add(-time.Minute)); err != nil {
		t.Fatalf("commitNextWindow: %v", err)
	}
	if err := m.openDueCommits(context.Background(), now); err != nil {
		t.Fatalf("openDueCommits: %v", err)
	}
	w, err := r.ActiveWindow(context.Background(), models.MarketETH, models.Duration5m)
	if err != nil {
		t.Fatalf("ActiveWindow: %v", err)
	}
	if w.Status != models.WindowCommitted {
		t.Fatalf("Status = %v, want still committed (within grace period, no price yet)", w.Status)
	}
}

func TestManager_LockDueWindows_RefundsUnfilledOrderAndCommitsNext(t *testing.T) {
	m, r, raw, fb, pool := testManagerDeps(t)
	now := time.Now()
	writeTestPrice(t, raw, "SOL", 150, now.UnixMilli())

	if err := m.commitNextWindow(context.Background(), models.MarketSOL, models.Duration5m, now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("commitNextWindow: %v", err)
	}
	w, err := r.ActiveWindow(context.Background(), models.MarketSOL, models.Duration5m)
	if err != nil {
		t.Fatalf("ActiveWindow: %v", err)
	}
	if err := r.RevealWindow(context.Background(), w.ID, decimal.NewFromInt(150), decimal.NewFromInt(150), now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("RevealWindow: %v", err)
	}
	userID := "test-lockdue-user"
	order := &models.Order{WindowID: w.ID, UserID: userID, Side: models.SideYes, Price: decimal.NewFromFloat(0.5), Size: decimal.NewFromInt(10)}
	orderID, err := r.CreateOrder(context.Background(), order)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	order.ID = orderID
	// Force the window's end_time into the past so lockDueWindows picks it up.
	if _, err := pool.Exec(context.Background(), `UPDATE prediction_windows SET end_time = now() - interval '1 second' WHERE id = $1`, w.ID); err != nil {
		t.Fatalf("force end_time: %v", err)
	}

	if err := m.lockDueWindows(context.Background(), time.Now()); err != nil {
		t.Fatalf("lockDueWindows: %v", err)
	}

	locked, err := r.GetWindow(context.Background(), w.ID)
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	if locked.Status != models.WindowLocked {
		t.Fatalf("Status = %v, want locked", locked.Status)
	}

	closedOrder, err := r.GetOrder(context.Background(), order.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if closedOrder.Status != models.OrderCancelled {
		t.Fatalf("unfilled order Status = %v, want cancelled after lock-time refund", closedOrder.Status)
	}

	if fb.unlockCallCount(userID, "BI2XUSD") == 0 {
		t.Fatal("lockDueWindows did not call Unlock for the unfilled order's remainder")
	}

	// A new committed window for the same market/duration should now exist.
	next, err := r.ActiveWindow(context.Background(), models.MarketSOL, models.Duration5m)
	if err != nil {
		t.Fatalf("ActiveWindow (next round): %v", err)
	}
	if next.ID == w.ID {
		t.Fatal("lockDueWindows did not commit a new next window for the same market/duration")
	}
}

func TestManager_SettleLockedWindows_PaysWinnerAndSkipsAlreadyPaid(t *testing.T) {
	m, r, raw, fb, _ := testManagerDeps(t)
	now := time.Now()
	writeTestPrice(t, raw, "BTC", 51000, now.UnixMilli())

	if err := m.commitNextWindow(context.Background(), models.MarketBTC, models.Duration15m, now.Add(-20*time.Minute)); err != nil {
		t.Fatalf("commitNextWindow: %v", err)
	}
	w, err := r.ActiveWindow(context.Background(), models.MarketBTC, models.Duration15m)
	if err != nil {
		t.Fatalf("ActiveWindow: %v", err)
	}
	if err := r.RevealWindow(context.Background(), w.ID, decimal.NewFromInt(50000), decimal.NewFromInt(50000), now.Add(-20*time.Minute)); err != nil {
		t.Fatalf("RevealWindow: %v", err)
	}
	if err := r.LockWindow(context.Background(), w.ID); err != nil {
		t.Fatalf("LockWindow: %v", err)
	}
	winnerID := "test-settle-winner"
	err = r.WithTx(context.Background(), func(tx pgx.Tx) error {
		return r.UpsertPosition(context.Background(), tx, w.ID, winnerID, models.SideYes, decimal.NewFromInt(10), decimal.NewFromFloat(0.5))
	})
	if err != nil {
		t.Fatalf("seed winning position: %v", err)
	}

	if err := m.settleLockedWindows(context.Background(), time.Now()); err != nil {
		t.Fatalf("settleLockedWindows: %v", err)
	}

	settled, err := r.GetWindow(context.Background(), w.ID)
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	if settled.Status != models.WindowSettled {
		t.Fatalf("Status = %v, want settled (51000 >= 50000 target, YES wins)", settled.Status)
	}

	pos, err := r.GetPosition(context.Background(), w.ID, winnerID, models.SideYes)
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}
	if !pos.PaidOut {
		t.Fatal("winning position PaidOut = false after settlement")
	}
	if fb.creditCallCount(winnerID, "BI2XUSD") != 1 {
		t.Fatalf("Credit called %d times for the winner, want exactly 1", fb.creditCallCount(winnerID, "BI2XUSD"))
	}

	// Re-running settlement should be a no-op — LockedWindows no longer
	// returns this window since it's settled — directly exercising the
	// PRED-M2 resumability guarantee: a position already marked paid is
	// never re-credited even if settlement somehow ran again.
	if err := m.settleLockedWindows(context.Background(), time.Now()); err != nil {
		t.Fatalf("settleLockedWindows (second call): %v", err)
	}
	if fb.creditCallCount(winnerID, "BI2XUSD") != 1 {
		t.Fatalf("Credit called %d times after a second settlement pass, want still exactly 1 (no double-pay)", fb.creditCallCount(winnerID, "BI2XUSD"))
	}
}
