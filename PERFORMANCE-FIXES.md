# Prediction Market Performance Issues — Diagnosis & Fix Plan

**Implemented.** This documents what was measured live, why it happened, and
the fixes that were applied and verified.

## Root cause (confirmed with live measurements)

`round.Manager.tick()` runs once per second and executes four steps **sequentially,
synchronously, on a single goroutine**:

```
openDueCommits → lockDueWindows → settleLockedWindows → broadcastTicks
```

Every one of these steps makes remote Postgres queries (and, for `lockDueWindows`,
remote HTTP calls to Dex-Backend to refund unfilled orders). Because they run
one after another with no timeout, **a single slow database round-trip stalls
the entire chain** — which means no ticks are broadcast for any of the 6
markets simultaneously, for however long that one call takes.

Live measurements from this session (instrumented with per-step timing):

| Step | Observed duration |
|---|---|
| `lock due windows` | 4.2s |
| `settle locked windows` | **33.9s** |
| `broadcast ticks` | 3.8s, later 12.8s |
| `open due commits` | 4.5s |

This matches the reported "30-40 second freeze" exactly, and explains why it
felt like the *whole page* froze rather than just one market: it did — all 6
markets stop ticking at once whenever any single step stalls.

The underlying trigger is intermittent network latency to the shared Aiven
Postgres instance — the same latency pattern already observed elsewhere this
session (matching-engine and Dex-Backend both had multi-second to ~90-second
slow startups against the same database during this session). This is not
unique to prediction-service; prediction-service just has no isolation
between the slow call and the live broadcast path, so it surfaces there as a
visible freeze instead of a quieter background retry.

## Why it's architecturally fragile

- No per-call timeout — a stalled query can block the loop indefinitely.
- No concurrency — one market's slow settlement blocks the other 5's ticks.
- No isolation between "bookkeeping" work (locking, settling, refunding) and
  the "live broadcast" path — they're forced into the same synchronous step
  even though users only ever notice the broadcast path stalling.

## Proposed fixes

1. **Add context timeouts to every DB/HTTP call in the tick chain.**
   A skipped tick (retried next second) is far better than a frozen one. A
   2-3 second timeout per call bounds the worst case per step instead of
   letting it run for 30+ seconds.

2. **Run per-market tick work concurrently** (one goroutine per
   market×duration, or at least per market), so one market's slow
   settlement/commit/lock work can't stall the other 5's broadcasts.

3. **Decouple the live broadcast path from settlement/lock/commit
   bookkeeping.** `broadcastTicks` should never be forced to wait behind
   `settleLockedWindows` in the same synchronous step — they don't need to
   run back-to-back at all. The broadcast path should be the one thing that
   NEVER waits on a slow database call if it can be avoided (see the Redis
   question below).

4. **Investigate the underlying Postgres latency itself** — check
   `pgxpool` connection pool settings (max connections, idle timeout,
   health checks), and whether the shared Aiven instance is simply
   under load or geographically distant from where these services run.
   This may reduce or eliminate the frequency of stalls without any of the
   above restructuring being strictly necessary — though 1-3 above are
   good defensive practice regardless of whether root-cause #4 is ever
   fully resolved, since a shared multi-tenant database will always have
   some risk of an occasional slow query.

## What was actually implemented

Fixes #1 and #3 (timeouts + decoupling the broadcast path from Postgres) were
implemented and verified live. #2 (per-market concurrency) and #4 (Postgres
pool/latency investigation) were not needed — see below.

### 1. Context timeouts on every tick step (`internal/round/manager.go`)

`tick()`'s `step()` wrapper now runs each of `openDueCommits`,
`lockDueWindows`, and `settleLockedWindows` under a 3-second
`context.WithTimeout`, instead of the raw per-second loop context. A stalled
call now fails that step and retries next tick instead of blocking
indefinitely — turning "can freeze for 30+ seconds" into "worst case ~3
seconds, then retries," per the plan above.

### 2. `broadcastTicks` no longer touches Postgres (`internal/roundcache`)

This is the fix that actually eliminates the freeze, not just bounds it. A
new package, `internal/roundcache`, mirrors each market/duration's active
window (`id`, `status`, `targetPrice`, `endTime`) in Redis — the same shared
`REDIS_SERVICE_URI` instance already used by `internal/index` (price feed)
and `internal/history` (chart ticks), under a new `prediction:activewindow:*`
key prefix. No new Redis instance was needed.

- `openDueCommits`, `lockDueWindows`, and `settleLockedWindows` write to the
  cache (`Manager.refreshCache`) every time they actually change a window's
  state (reveal → open, lock → locked, settle → settled). `ensureActiveWindow`
  also seeds the cache from Postgres on boot, so a restart doesn't leave
  `broadcastTicks` blind until the next state change.
- `broadcastTicks` now reads exclusively from `roundcache.Cache.Get`, never
  from `repo.ActiveWindow`. A cache miss (Redis hiccup, or a window that
  hasn't been revealed yet) just skips that market/duration for one tick —
  it never falls back to Postgres and never blocks the other markets.
- The cache entry has a 2-minute TTL as a safety net, so a bug that stops
  refreshing it can't broadcast a stale window forever.
- Postgres remains the sole source of truth for everything durable: orders,
  matching, balance locks/credits, the commit-reveal target, and settlement
  records are all unchanged and still fully synchronous. Only the
  presentation-layer "what should the live tick show right now" read moved
  to Redis.

### Verified live

- `go build ./...`, `go vet ./...`, `gofmt -l .` all clean.
- Ran the service against the real shared Aiven Postgres/Redis instances.
  While `lockDueWindows`/`settleLockedWindows` repeatedly took 1-1.8s each
  working through a backlog of overdue windows, `broadcastTicks` stayed at
  the same ~600ms or under for the whole run and **never appeared as a slow
  step tied to the backlog** — confirming the two paths are now fully
  decoupled.
- WebSocket ticks arrived within ~1 second of connecting, for all three
  live 15-minute markets simultaneously, throughout the backlog processing.
- `/prediction/windows` and `/prediction/history?windowId=...` both continued
  to serve correct, current data throughout.
- No errors or panics in the logs during the test run.

### Not needed

- **#2 (per-market concurrency)**: became moot once broadcasting no longer
  depends on the bookkeeping steps at all — there's nothing left for one
  market's slow settlement to block.
- **#4 (Postgres pool/latency investigation)**: the observed 1-1.8s Postgres
  round-trips during backlog processing no longer matter, since they can't
  reach the live broadcast path. Worth revisiting only if lock/settle/commit
  latency itself becomes a problem (e.g. it starts exceeding the 3s step
  timeout under normal, non-backlog conditions).
