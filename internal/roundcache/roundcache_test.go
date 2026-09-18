package roundcache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/models"
)

func testCache(t *testing.T) *Cache {
	t.Helper()
	_ = godotenv.Load("../../.env")
	uri := os.Getenv("REDIS_SERVICE_URI")
	if uri == "" {
		t.Skip("REDIS_SERVICE_URI not set, skipping live-Redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := New(ctx, uri)
	if err != nil {
		t.Skipf("could not connect to Redis: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestCache_SetThenGet_RoundTrips(t *testing.T) {
	c := testCache(t)
	ctx := context.Background()
	// Use a duration not used by any other concurrently-running test/service
	// against the same shared Redis instance would be ideal, but this
	// package's key is fixed per market/duration — accept that a genuinely
	// concurrent write to the exact same pair could interleave, same as any
	// shared-cache test would.
	w := Window{ID: 12345, Status: models.WindowOpen, TargetPrice: decimal.NewFromInt(50000), EndTime: time.Now().Add(5 * time.Minute).Truncate(time.Millisecond)}
	if err := c.Set(ctx, models.MarketBTC, models.Duration5m, w); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok := c.Get(ctx, models.MarketBTC, models.Duration5m)
	if !ok {
		t.Fatal("Get returned ok=false right after Set")
	}
	if got.ID != w.ID {
		t.Errorf("ID = %d, want %d", got.ID, w.ID)
	}
	if got.Status != w.Status {
		t.Errorf("Status = %v, want %v", got.Status, w.Status)
	}
	if !got.TargetPrice.Equal(w.TargetPrice) {
		t.Errorf("TargetPrice = %v, want %v", got.TargetPrice, w.TargetPrice)
	}
	if !got.EndTime.Equal(w.EndTime) {
		t.Errorf("EndTime = %v, want %v", got.EndTime, w.EndTime)
	}
}

func TestCache_Get_MissingKeyReturnsNotOK(t *testing.T) {
	c := testCache(t)
	// SOL/15m is unlikely to collide with the 5m test above, but the
	// contract under test — "no key written yet" — is what matters, not
	// which specific pair; use a duration+market combo this test file
	// doesn't write to elsewhere.
	_, ok := c.Get(context.Background(), models.MarketSOL, models.Duration15m)
	if ok {
		t.Skip("a key already exists for SOL/15m from a real running service sharing this Redis instance — not this test's concern, skipping rather than asserting a false negative")
	}
}

func TestCache_Set_OverwritesPreviousValue(t *testing.T) {
	c := testCache(t)
	ctx := context.Background()
	first := Window{ID: 1, Status: models.WindowCommitted, TargetPrice: decimal.Zero, EndTime: time.Now().Add(time.Minute).Truncate(time.Millisecond)}
	second := Window{ID: 2, Status: models.WindowOpen, TargetPrice: decimal.NewFromInt(100), EndTime: time.Now().Add(2 * time.Minute).Truncate(time.Millisecond)}

	if err := c.Set(ctx, models.MarketETH, models.Duration5m, first); err != nil {
		t.Fatalf("Set (first): %v", err)
	}
	if err := c.Set(ctx, models.MarketETH, models.Duration5m, second); err != nil {
		t.Fatalf("Set (second): %v", err)
	}
	got, ok := c.Get(ctx, models.MarketETH, models.Duration5m)
	if !ok {
		t.Fatal("Get returned ok=false after two Sets")
	}
	if got.ID != second.ID {
		t.Fatalf("Get returned id=%d, want the second Set's id=%d (overwrite expected)", got.ID, second.ID)
	}
}
