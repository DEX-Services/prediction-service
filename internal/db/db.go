// Package db owns this service's own Postgres tables. It shares the same
// Aiven Postgres instance as Dex-Backend/matching-engine (same
// POSTGRES_SERVICE_URI) but only ever creates/writes its own
// prediction_-prefixed tables here — real balance movement always goes
// through Dex-Backend's HTTP API (see internal/backendclient), never direct
// SQL into tables another service owns.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens the pool and runs all migrations in order, matching the
// inline-Go-string migration pattern already used in Dex-Backend and
// matching-engine (no separate migration framework/files in this codebase).
func Connect(ctx context.Context, uri string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db ping: %w", err)
	}

	migrations := []struct {
		name string
		sql  string
	}{
		{"prediction windows table", ensureWindowsTable},
		{"prediction orders table", ensureOrdersTable},
		{"prediction positions table", ensurePositionsTable},
		{"prediction fills table", ensureFillsTable},
		{"prediction settlements table", ensureSettlementsTable},
		{"prediction positions paid_out column", addPositionsPaidOutColumn},
	}
	for _, m := range migrations {
		if _, err := pool.Exec(ctx, m.sql); err != nil {
			pool.Close()
			return nil, fmt.Errorf("migration %q: %w", m.name, err)
		}
	}
	return pool, nil
}

const ensureWindowsTable = `
CREATE TABLE IF NOT EXISTS prediction_windows (
    id                 BIGSERIAL PRIMARY KEY,
    market             TEXT NOT NULL CHECK (market IN ('BTC', 'ETH', 'SOL')),
    duration           TEXT NOT NULL CHECK (duration IN ('5m', '15m')),
    commit_hash        TEXT NOT NULL,
    -- salt/offset_bps are written at commit time (not left NULL) so the
    -- Round Manager survives a restart between commit and reveal without
    -- losing the random target; they're PUBLICLY REVEALED only once
    -- revealed_at is set (via the API's window view), enforcing the
    -- commit-reveal fairness guarantee at the read layer instead of at
    -- the storage layer.
    salt               TEXT NOT NULL,
    offset_bps         BIGINT NOT NULL,
    opening_price      NUMERIC(38, 18),
    target_price       NUMERIC(38, 18),
    resolution_price   NUMERIC(38, 18),
    status             TEXT NOT NULL DEFAULT 'committed'
                           CHECK (status IN ('committed', 'open', 'locked', 'settled')),
    commit_time        TIMESTAMPTZ NOT NULL,
    start_time         TIMESTAMPTZ NOT NULL,
    end_time           TIMESTAMPTZ NOT NULL,
    revealed_at        TIMESTAMPTZ,
    settled_at         TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_prediction_windows_market_duration_status
    ON prediction_windows (market, duration, status);
CREATE INDEX IF NOT EXISTS idx_prediction_windows_end_time
    ON prediction_windows (end_time) WHERE status IN ('open', 'locked');
`

const ensureOrdersTable = `
CREATE TABLE IF NOT EXISTS prediction_orders (
    id            BIGSERIAL PRIMARY KEY,
    window_id     BIGINT NOT NULL REFERENCES prediction_windows(id),
    user_id       TEXT NOT NULL,
    side          TEXT NOT NULL CHECK (side IN ('yes', 'no')),
    price         NUMERIC(6, 4) NOT NULL CHECK (price > 0 AND price < 1),
    size          NUMERIC(38, 18) NOT NULL CHECK (size > 0),
    filled_size   NUMERIC(38, 18) NOT NULL DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'filled', 'cancelled')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_prediction_orders_window_side_status_price
    ON prediction_orders (window_id, side, status, price, created_at);
CREATE INDEX IF NOT EXISTS idx_prediction_orders_user
    ON prediction_orders (user_id, created_at DESC);
`

const ensurePositionsTable = `
CREATE TABLE IF NOT EXISTS prediction_positions (
    id           BIGSERIAL PRIMARY KEY,
    window_id    BIGINT NOT NULL REFERENCES prediction_windows(id),
    user_id      TEXT NOT NULL,
    side         TEXT NOT NULL CHECK (side IN ('yes', 'no')),
    shares       NUMERIC(38, 18) NOT NULL DEFAULT 0,
    avg_price    NUMERIC(6, 4) NOT NULL DEFAULT 0,
    realized     NUMERIC(38, 18) NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (window_id, user_id, side)
);
CREATE INDEX IF NOT EXISTS idx_prediction_positions_user
    ON prediction_positions (user_id, updated_at DESC);
`

// addPositionsPaidOutColumn backs PRED-M2's idempotent settlement payout: a
// winning position is only ever Credit()-ed once, resumable if the
// settlement loop crashes or a single Credit call fails partway through a
// window's payout run.
const addPositionsPaidOutColumn = `
ALTER TABLE prediction_positions ADD COLUMN IF NOT EXISTS paid_out BOOLEAN NOT NULL DEFAULT false;
`

const ensureFillsTable = `
CREATE TABLE IF NOT EXISTS prediction_fills (
    id              BIGSERIAL PRIMARY KEY,
    window_id       BIGINT NOT NULL REFERENCES prediction_windows(id),
    maker_order_id  BIGINT NOT NULL REFERENCES prediction_orders(id),
    taker_order_id  BIGINT NOT NULL REFERENCES prediction_orders(id),
    price           NUMERIC(6, 4) NOT NULL,
    size            NUMERIC(38, 18) NOT NULL,
    maker_fee       NUMERIC(38, 18) NOT NULL,
    taker_fee       NUMERIC(38, 18) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_prediction_fills_window
    ON prediction_fills (window_id, created_at);
`

const ensureSettlementsTable = `
CREATE TABLE IF NOT EXISTS prediction_settlements (
    id                BIGSERIAL PRIMARY KEY,
    window_id         BIGINT NOT NULL UNIQUE REFERENCES prediction_windows(id),
    winning_side      TEXT NOT NULL CHECK (winning_side IN ('yes', 'no')),
    resolution_price  NUMERIC(38, 18) NOT NULL,
    target_price      NUMERIC(38, 18) NOT NULL,
    total_paid_out    NUMERIC(38, 18) NOT NULL,
    settled_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
`
