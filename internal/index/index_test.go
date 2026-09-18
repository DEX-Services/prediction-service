package index

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

func testReader(t *testing.T, maxAge time.Duration) (*Reader, *redis.Client) {
	t.Helper()
	_ = godotenv.Load("../../.env")
	uri := os.Getenv("REDIS_SERVICE_URI")
	if uri == "" {
		t.Skip("REDIS_SERVICE_URI not set, skipping live-Redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := New(ctx, uri, "test-index-prefix", maxAge)
	if err != nil {
		t.Skipf("could not connect to Redis: %v", err)
	}
	t.Cleanup(func() { r.Close() })

	opts, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse redis URI for direct write access: %v", err)
	}
	raw := redis.NewClient(opts)
	t.Cleanup(func() { raw.Close() })
	return r, raw
}

func writePrice(t *testing.T, raw *redis.Client, key string, last float64, timestampMs int64) {
	t.Helper()
	data, err := json.Marshal(payload{Last: last, TimestampMs: timestampMs, ChangePercent: 1.5, High24h: last + 100, Low24h: last - 100, QuoteVolume: 1000})
	if err != nil {
		t.Fatalf("marshal test payload: %v", err)
	}
	if err := raw.Set(context.Background(), key, data, time.Minute).Err(); err != nil {
		t.Fatalf("write test price to redis: %v", err)
	}
	t.Cleanup(func() { raw.Del(context.Background(), key) })
}

func TestReader_Get_FreshPrice(t *testing.T) {
	r, raw := testReader(t, 5*time.Second)
	now := time.Now().UnixMilli()
	writePrice(t, raw, "test-index-prefix:BTC", 50000, now)

	snap := r.Get(context.Background(), "btc", now) // lower-case input: Get upper-cases internally
	if !snap.Fresh {
		t.Fatal("Get returned Fresh=false for a price published at exactly now")
	}
	if !snap.Price.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("Price = %v, want 50000", snap.Price)
	}
	if snap.AgeMs != 0 {
		t.Fatalf("AgeMs = %d, want 0 (published at now)", snap.AgeMs)
	}
}

func TestReader_Get_StalePriceBeyondMaxAge(t *testing.T) {
	r, raw := testReader(t, 5*time.Second)
	now := time.Now().UnixMilli()
	writePrice(t, raw, "test-index-prefix:ETH", 3000, now-10000) // 10s old, maxAge is 5s

	snap := r.Get(context.Background(), "ETH", now)
	if snap.Fresh {
		t.Fatal("Get returned Fresh=true for a price older than maxAge")
	}
}

func TestReader_Get_MissingKeyIsNotFresh(t *testing.T) {
	r, _ := testReader(t, 5*time.Second)
	snap := r.Get(context.Background(), "NOSUCHASSET-XYZ", time.Now().UnixMilli())
	if snap.Fresh {
		t.Fatal("Get returned Fresh=true for a key that was never written")
	}
}

func TestReader_Get_NonPositivePriceIsNotFresh(t *testing.T) {
	r, raw := testReader(t, 5*time.Second)
	now := time.Now().UnixMilli()
	writePrice(t, raw, "test-index-prefix:SOL", 0, now)

	snap := r.Get(context.Background(), "SOL", now)
	if snap.Fresh {
		t.Fatal("Get returned Fresh=true for a non-positive (zero) price")
	}
}

func TestReader_Get_MalformedJSONIsNotFresh(t *testing.T) {
	r, raw := testReader(t, 5*time.Second)
	key := "test-index-prefix:MALFORMED"
	if err := raw.Set(context.Background(), key, []byte("not json"), time.Minute).Err(); err != nil {
		t.Fatalf("write malformed value: %v", err)
	}
	t.Cleanup(func() { raw.Del(context.Background(), key) })

	snap := r.Get(context.Background(), "MALFORMED", time.Now().UnixMilli())
	if snap.Fresh {
		t.Fatal("Get returned Fresh=true for malformed JSON")
	}
}

func TestReader_GetExact_PreservesCaseUnlikeGet(t *testing.T) {
	r, raw := testReader(t, 5*time.Second)
	now := time.Now().UnixMilli()
	// Get would upper-case this to "AAPL.US" and miss the key entirely;
	// GetExact must look up the exact case-sensitive ticker as given.
	writePrice(t, raw, "test-index-prefix:AAPL.us", 200, now)

	snap := r.GetExact(context.Background(), "AAPL.us", now)
	if !snap.Fresh {
		t.Fatal("GetExact returned Fresh=false for a key written with the exact same case")
	}

	missed := r.Get(context.Background(), "AAPL.us", now)
	if missed.Fresh {
		t.Fatal("Get (case-insensitive upper-casing) unexpectedly found a lower-case-suffixed key — GetExact's whole reason to exist is that Get should NOT find this")
	}
}

func TestReader_Get_NegativeAgeClampedToZero(t *testing.T) {
	// A price timestamped slightly in the future relative to nowMs (clock
	// skew between this service and Price-Fetcher) shouldn't produce a
	// negative age.
	r, raw := testReader(t, 5*time.Second)
	now := time.Now().UnixMilli()
	writePrice(t, raw, "test-index-prefix:FUTURE", 100, now+2000)

	snap := r.Get(context.Background(), "FUTURE", now)
	if snap.AgeMs < 0 {
		t.Fatalf("AgeMs = %d, want clamped to >= 0", snap.AgeMs)
	}
	if !snap.Fresh {
		t.Fatal("a price timestamped slightly in the future should still count as fresh (age clamped to 0, well under maxAge)")
	}
}
