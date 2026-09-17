// Package roundcache mirrors each market/duration's active window in Redis so
// the live tick-broadcast path never has to call Postgres. It uses the same
// shared Redis instance as internal/index and internal/history (same
// REDIS_SERVICE_URI) — this data is small, high-frequency, and disposable:
// if the key is missing or stale, the caller just skips broadcasting that
// market/duration for one tick and picks it up again next second once
// round.Manager's bookkeeping steps refresh it. Postgres remains the sole
// source of truth; this cache is refreshed every time Postgres state
// actually changes (commit, reveal, lock, settle), never the other way.
package roundcache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/models"
)

// Window is the subset of models.Window the live broadcast path needs.
type Window struct {
	ID          int64               `json:"id"`
	Status      models.WindowStatus `json:"status"`
	TargetPrice decimal.Decimal     `json:"target_price"`
	EndTime     time.Time           `json:"end_time"`
}

// Cache reads/writes the active-window mirror in Redis.
type Cache struct {
	rdb *redis.Client
}

// New connects to Redis using uri (rediss:// for TLS) — the same shared URI
// used elsewhere in this service.
func New(ctx context.Context, uri string) (*Cache, error) {
	if uri == "" {
		return nil, fmt.Errorf("REDIS_SERVICE_URI is not set")
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("parse redis URI: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Cache{rdb: rdb}, nil
}

func key(market models.Market, duration models.Duration) string {
	return fmt.Sprintf("prediction:activewindow:%s:%s", market, duration)
}

// ttl bounds how long a stale entry can survive if a restart or bug ever
// stops refreshing it — long enough to cover any normal gap between writes,
// short enough that a genuinely stuck manager doesn't broadcast a window
// forever.
const ttl = 2 * time.Minute

// Set stores/refreshes market/duration's active window. Called from
// round.Manager whenever the underlying Postgres row actually changes state
// (commit, reveal, lock, settle) — never from the broadcast path itself.
func (c *Cache) Set(ctx context.Context, market models.Market, duration models.Duration, w Window) error {
	data, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, key(market, duration), data, ttl).Err()
}

// Get returns the cached active window for market/duration, or ok=false if
// absent/expired/malformed — callers must treat that the same as "nothing to
// broadcast this tick" rather than falling back to Postgres, so a Redis
// hiccup degrades to a skipped tick instead of reintroducing the stall this
// cache exists to avoid.
func (c *Cache) Get(ctx context.Context, market models.Market, duration models.Duration) (Window, bool) {
	raw, err := c.rdb.Get(ctx, key(market, duration)).Bytes()
	if err != nil {
		return Window{}, false
	}
	var w Window
	if err := json.Unmarshal(raw, &w); err != nil {
		return Window{}, false
	}
	return w, true
}

// Close releases the Redis connection.
func (c *Cache) Close() error { return c.rdb.Close() }
