package round

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/backendclient"
	"github.com/dex/prediction-service/internal/db"
	"github.com/dex/prediction-service/internal/models"
	"github.com/dex/prediction-service/internal/repo"
)

// testPool connects to the real Postgres instance this service shares with
// the rest of the platform, skipping (not failing) when unavailable — same
// pattern Dex-Backend's internal/repo tests already use.
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

// fakeBackend is an in-memory stand-in for Dex-Backend's /internal/balance/*
// endpoints, tracking one running signed balance per user+asset so a test
// can assert the exact economics a real fill produced (locked cost debited,
// fees collected) without needing a live Dex-Backend instance.
type fakeBackend struct {
	mu       sync.Mutex
	balances map[string]decimal.Decimal // key: userID+"|"+asset
	fees     map[string]decimal.Decimal // key: userID+"|"+asset, cumulative fee revenue
	calls    map[string]int             // key: op+"|"+userID+"|"+asset, call count (used by manager_test.go)
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{balances: map[string]decimal.Decimal{}, fees: map[string]decimal.Decimal{}, calls: map[string]int{}}
}

func (f *fakeBackend) callCount(op, userID, asset string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op+"|"+f.key(userID, asset)]
}

func (f *fakeBackend) unlockCallCount(userID, asset string) int {
	return f.callCount("unlock", userID, asset)
}
func (f *fakeBackend) creditCallCount(userID, asset string) int {
	return f.callCount("credit", userID, asset)
}

// rawUnits mirrors backendclient.ToRawUnits(amount) but parses the result
// back into a decimal.Decimal for arithmetic/comparison against what
// fakeBackend recorded (which is itself parsed from the raw-string wire
// values matcher.go actually sends).
func rawUnits(t *testing.T, amount decimal.Decimal) decimal.Decimal {
	t.Helper()
	d, err := decimal.NewFromString(backendclient.ToRawUnits(amount))
	if err != nil {
		t.Fatalf("rawUnits: %v", err)
	}
	return d
}

func (f *fakeBackend) key(userID, asset string) string { return userID + "|" + asset }

func (f *fakeBackend) balance(userID, asset string) decimal.Decimal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.balances[f.key(userID, asset)]
}

func (f *fakeBackend) fee(userID, asset string) decimal.Decimal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fees[f.key(userID, asset)]
}

func (f *fakeBackend) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/user/ensure", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handleBalanceOp := func(op string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var req struct{ UserID, Asset, Amount string }
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			amt, err := decimal.NewFromString(req.Amount)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			f.mu.Lock()
			key := f.key(req.UserID, req.Asset)
			f.calls[op+"|"+key]++
			switch op {
			case "lock":
				// Lock doesn't move the running balance in this fake — it
				// only needs to succeed so PlaceOrder proceeds; the actual
				// economics land via the signed Credit call in settleFill,
				// mirroring how Dex-Backend's real lock/credit split works
				// (lock reserves, a later signed credit is what actually
				// moves the number this test asserts on).
			case "credit":
				f.balances[key] = f.balances[key].Add(amt)
			case "fee":
				f.fees[key] = f.fees[key].Add(amt)
			}
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}
	}
	mux.HandleFunc("/internal/balance/lock", handleBalanceOp("lock"))
	mux.HandleFunc("/internal/balance/unlock", handleBalanceOp("unlock"))
	mux.HandleFunc("/internal/balance/credit", handleBalanceOp("credit"))
	mux.HandleFunc("/internal/balance/fee", handleBalanceOp("fee"))
	mux.HandleFunc("/internal/balance/available", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"available": "1000000"})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// newTestWindow inserts a minimal, valid open window row for the matcher's
// FK-constrained order/position/fill inserts to attach to.
func newTestWindow(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO prediction_windows
			(market, duration, commit_hash, salt, offset_bps, opening_price, target_price, status, commit_time, start_time, end_time)
		VALUES ($1, $2, 'testhash', 'testsalt', 10, 50000, 50000, 'open', $3, $3, $4)
		RETURNING id`,
		models.MarketBTC, models.Duration5m, now, now.Add(5*time.Minute),
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert test window: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM prediction_fills WHERE window_id = $1`, id)
		pool.Exec(context.Background(), `DELETE FROM prediction_positions WHERE window_id = $1`, id)
		pool.Exec(context.Background(), `DELETE FROM prediction_orders WHERE window_id = $1`, id)
		pool.Exec(context.Background(), `DELETE FROM prediction_windows WHERE id = $1`, id)
	})
	return id
}

func TestMatcher_PlaceOrder_MatchesAndSettlesFillEconomics(t *testing.T) {
	pool := testPool(t)
	windowID := newTestWindow(t, pool)
	fb := newFakeBackend()
	srv := fb.server(t)
	client := backendclient.NewForTest(srv.URL, "test-secret", srv.Client())
	m := NewMatcher(repo.New(pool), client)
	ctx := context.Background()

	makerID := fmt.Sprintf("test-maker-%d", time.Now().UnixNano())
	takerID := fmt.Sprintf("test-taker-%d", time.Now().UnixNano())

	// Maker rests a YES order at 0.60; a NO taker at 0.45 crosses it
	// (0.60 + 0.45 = 1.05 >= 1), filling fully at the maker's price per
	// price-time priority (PlaceOrder's doc comment).
	makerOrder := &models.Order{WindowID: windowID, UserID: makerID, Side: models.SideYes, Price: decimal.NewFromFloat(0.60), Size: decimal.NewFromInt(100)}
	_, makerFills, err := m.PlaceOrder(ctx, makerOrder)
	if err != nil {
		t.Fatalf("place maker order: %v", err)
	}
	if len(makerFills) != 0 {
		t.Fatalf("maker order should rest unfilled (no opposing book yet), got %d fills", len(makerFills))
	}

	takerOrder := &models.Order{WindowID: windowID, UserID: takerID, Side: models.SideNo, Price: decimal.NewFromFloat(0.45), Size: decimal.NewFromInt(100)}
	_, fills, err := m.PlaceOrder(ctx, takerOrder)
	if err != nil {
		t.Fatalf("place taker order: %v", err)
	}
	if len(fills) != 1 {
		t.Fatalf("expected exactly 1 fill, got %d", len(fills))
	}
	fill := fills[0]
	if !fill.Price.Equal(decimal.NewFromFloat(0.60)) {
		t.Fatalf("fill price = %v, want 0.60 (the maker/resting order's price)", fill.Price)
	}
	if !fill.Size.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("fill size = %v, want 100 (full match, equal sizes)", fill.Size)
	}

	// Maker is YES @ 0.60 for size 100 -> cost = 60, fee = 60*0.00015 = 0.009.
	// Taker is NO, complementary cost = (1-0.60)*100 = 40, fee = 40*0.00045 = 0.018.
	// Amounts observed by fakeBackend are in raw units (see
	// backendclient.ToRawUnits — everything crossing that boundary is raw,
	// 6-decimal-scaled), so the expected values are raw-scaled here too.
	wantMakerDebit := rawUnits(t, decimal.NewFromFloat(60).Add(decimal.NewFromFloat(60).Mul(makerFeeRate))).Neg()
	wantTakerDebit := rawUnits(t, decimal.NewFromFloat(40).Add(decimal.NewFromFloat(40).Mul(takerFeeRate))).Neg()

	gotMakerBalance := fb.balance(makerID, "BI2XUSD")
	gotTakerBalance := fb.balance(takerID, "BI2XUSD")
	if !gotMakerBalance.Equal(wantMakerDebit) {
		t.Errorf("maker balance = %s, want %s", gotMakerBalance, wantMakerDebit)
	}
	if !gotTakerBalance.Equal(wantTakerDebit) {
		t.Errorf("taker balance = %s, want %s", gotTakerBalance, wantTakerDebit)
	}

	wantMakerFee := rawUnits(t, decimal.NewFromFloat(60).Mul(makerFeeRate))
	wantTakerFee := rawUnits(t, decimal.NewFromFloat(40).Mul(takerFeeRate))
	if !fb.fee(makerID, "BI2XUSD").Equal(wantMakerFee) {
		t.Errorf("maker fee collected = %s, want %s", fb.fee(makerID, "BI2XUSD"), wantMakerFee)
	}
	if !fb.fee(takerID, "BI2XUSD").Equal(wantTakerFee) {
		t.Errorf("taker fee collected = %s, want %s", fb.fee(takerID, "BI2XUSD"), wantTakerFee)
	}

	// Both orders should now show as fully filled — verifies FillOrder's
	// status transition, not just the fill record itself.
	makerPos, err := repo.New(pool).GetPosition(ctx, windowID, makerID, models.SideYes)
	if err != nil {
		t.Fatalf("get maker position: %v", err)
	}
	if !makerPos.Shares.Equal(decimal.NewFromInt(100)) {
		t.Errorf("maker position shares = %s, want 100", makerPos.Shares)
	}
	takerPos, err := repo.New(pool).GetPosition(ctx, windowID, takerID, models.SideNo)
	if err != nil {
		t.Fatalf("get taker position: %v", err)
	}
	if !takerPos.Shares.Equal(decimal.NewFromInt(100)) {
		t.Errorf("taker position shares = %s, want 100", takerPos.Shares)
	}
}

func TestMatcher_PlaceOrder_IncompatiblePricesRestUnfilled(t *testing.T) {
	pool := testPool(t)
	windowID := newTestWindow(t, pool)
	fb := newFakeBackend()
	srv := fb.server(t)
	client := backendclient.NewForTest(srv.URL, "test-secret", srv.Client())
	m := NewMatcher(repo.New(pool), client)
	ctx := context.Background()

	makerID := fmt.Sprintf("test-maker2-%d", time.Now().UnixNano())
	takerID := fmt.Sprintf("test-taker2-%d", time.Now().UnixNano())

	// YES @ 0.40 + NO @ 0.40 = 0.80 < 1: not compatible, must NOT match
	// (the p+q>=1 rule documented on PlaceOrder).
	if _, _, err := m.PlaceOrder(ctx, &models.Order{WindowID: windowID, UserID: makerID, Side: models.SideYes, Price: decimal.NewFromFloat(0.40), Size: decimal.NewFromInt(50)}); err != nil {
		t.Fatalf("place maker order: %v", err)
	}
	_, fills, err := m.PlaceOrder(ctx, &models.Order{WindowID: windowID, UserID: takerID, Side: models.SideNo, Price: decimal.NewFromFloat(0.40), Size: decimal.NewFromInt(50)})
	if err != nil {
		t.Fatalf("place taker order: %v", err)
	}
	if len(fills) != 0 {
		t.Fatalf("incompatible prices (sum < 1) should not match, got %d fills", len(fills))
	}
}
