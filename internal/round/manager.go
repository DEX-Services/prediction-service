// Package round owns the 6 always-rolling prediction rounds (BTC/ETH/SOL x
// 5m/15m): the commit-reveal random target scheme, live pricing, order
// matching, and automatic settlement.
package round

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/backendclient"
	"github.com/dex/prediction-service/internal/history"
	"github.com/dex/prediction-service/internal/index"
	"github.com/dex/prediction-service/internal/models"
	"github.com/dex/prediction-service/internal/repo"
	"github.com/dex/prediction-service/internal/roundcache"
)

// Tick is a live pricing update for one window, broadcast every second.
type Tick struct {
	WindowID      int64
	Market        models.Market
	Duration      models.Duration
	CurrentPrice  decimal.Decimal
	TargetPrice   decimal.Decimal
	YesPrice      decimal.Decimal
	NoPrice       decimal.Decimal
	TimeRemaining time.Duration
	Status        models.WindowStatus
}

var allMarkets = []models.Market{models.MarketBTC, models.MarketETH, models.MarketSOL}
var allDurations = []models.Duration{models.Duration5m, models.Duration15m}

// Manager runs the 6 always-rolling rounds: it opens the next round the
// instant one locks, so there is no gap in which a market has no active
// window.
type Manager struct {
	repo    *repo.Repo
	prices  *index.Reader
	matcher *Matcher
	client  *backendclient.Client
	history *history.Store
	cache   *roundcache.Cache
	log     *slog.Logger

	onTick func(Tick)
}

func NewManager(r *repo.Repo, prices *index.Reader, matcher *Matcher, client *backendclient.Client, hist *history.Store, cache *roundcache.Cache, log *slog.Logger, onTick func(Tick)) *Manager {
	return &Manager{repo: r, prices: prices, matcher: matcher, client: client, history: hist, cache: cache, log: log, onTick: onTick}
}

// refreshCache mirrors w's live-relevant state into Redis so broadcastTicks
// can read it without touching Postgres. Best-effort: a failure here just
// means the cache serves a stale/expired value next tick (skipped broadcast,
// not a stall) rather than blocking any bookkeeping step.
func (m *Manager) refreshCache(ctx context.Context, w *models.Window) {
	if m.cache == nil {
		return
	}
	cw := roundcache.Window{ID: w.ID, Status: w.Status, EndTime: w.EndTime}
	if w.TargetPrice.Valid {
		cw.TargetPrice = w.TargetPrice.Decimal
	}
	if err := m.cache.Set(ctx, w.Market, w.Duration, cw); err != nil {
		m.log.Error("refresh round cache", "window_id", w.ID, "err", err)
	}
}

// Start ensures all 6 market/duration streams have an active window, then
// runs the tick/lock/settle loop until ctx is cancelled.
func (m *Manager) Start(ctx context.Context) error {
	for _, market := range allMarkets {
		for _, duration := range allDurations {
			if err := m.ensureActiveWindow(ctx, market, duration); err != nil {
				return fmt.Errorf("ensure active window %s/%s: %w", market, duration, err)
			}
		}
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			m.tick(ctx, now)
		}
	}
}

// tickStepTimeout bounds each bookkeeping step's worth of DB/HTTP work per
// tick. A skipped tick (retried next second, since these steps are all
// idempotent re-scans of "what's due now") is far better than a step that
// blocks the whole per-second loop for 30+ seconds on one slow round-trip —
// see PERFORMANCE-FIXES.md fix #1.
const tickStepTimeout = 3 * time.Second

func (m *Manager) tick(ctx context.Context, now time.Time) {
	step := func(name string, fn func(context.Context) error) {
		stepCtx, cancel := context.WithTimeout(ctx, tickStepTimeout)
		defer cancel()
		start := time.Now()
		if err := fn(stepCtx); err != nil {
			m.log.Error(name, "err", err)
		}
		if d := time.Since(start); d > 500*time.Millisecond {
			m.log.Warn("slow tick step", "step", name, "duration", d)
		}
	}
	step("open due commits", func(c context.Context) error { return m.openDueCommits(c, now) })
	step("lock due windows", func(c context.Context) error { return m.lockDueWindows(c, now) })
	step("settle locked windows", func(c context.Context) error { return m.settleLockedWindows(c, now) })
	// broadcastTicks no longer touches Postgres (see its comment), so it
	// doesn't need the same timeout treatment — Redis calls it does make are
	// already fast and non-blocking for the other steps regardless.
	step("broadcast ticks", func(c context.Context) error { m.broadcastTicks(c, now); return nil })
}

// ensureActiveWindow creates a committed window for market/duration if none
// exists yet (first boot only; afterward the roll happens in openDueCommits).
// It also seeds roundcache from the existing Postgres row on every boot —
// without this, a restart would leave broadcastTicks blind for that
// market/duration until its next reveal/lock/settle event happens to refresh
// the cache naturally.
func (m *Manager) ensureActiveWindow(ctx context.Context, market models.Market, duration models.Duration) error {
	w, err := m.repo.ActiveWindow(ctx, market, duration)
	if err == nil {
		m.refreshCache(ctx, w)
		return nil
	}
	if !errors.Is(err, repo.ErrNotFound) {
		return err
	}
	return m.commitNextWindow(ctx, market, duration, time.Now())
}

// commitNextWindow publishes the commit hash for the next round, to start
// immediately (startTime = now). offsetBps/salt are written to the database
// right away (see CreateWindow) so a restart between commit and reveal can't
// lose them — the fairness guarantee is enforced by never exposing them
// through the API before reveal, not by keeping them out of storage.
func (m *Manager) commitNextWindow(ctx context.Context, market models.Market, duration models.Duration, startTime time.Time) error {
	offsetBps, salt, hash, err := NewCommitment(duration)
	if err != nil {
		return fmt.Errorf("new commitment: %w", err)
	}

	w := &models.Window{
		Market:     market,
		Duration:   duration,
		CommitHash: hash,
		Status:     models.WindowCommitted,
		CommitTime: time.Now(),
		StartTime:  startTime,
		EndTime:    startTime.Add(duration.Window()),
	}
	_, err = m.repo.CreateWindow(ctx, w, offsetBps, salt)
	return err
}

// revealGracePeriod bounds how long a committed window waits for a fresh
// price before revealing anyway with the last-known (stale) price rather
// than retrying silently forever. Without this, a genuine upstream feed gap
// at the exact moment of rollover could leave a round stuck in "committed"
// indefinitely — no ticks are ever broadcast for a committed window (see
// broadcastTicks), so the frontend would show nothing but a placeholder
// with no way to tell the difference between "about to open" and "stuck".
const revealGracePeriod = 5 * time.Second

func (m *Manager) openDueCommits(ctx context.Context, now time.Time) error {
	windows, err := m.repo.DueCommits(ctx, now)
	if err != nil {
		return err
	}
	for _, w := range windows {
		snap := m.prices.Get(ctx, string(w.Market), now.UnixMilli())
		if !snap.Fresh {
			waited := now.Sub(w.CommitTime)
			if waited < revealGracePeriod || !snap.Price.IsPositive() {
				// Still within the normal reveal window, or there's no
				// price at all to fall back to (feed genuinely never
				// published this asset) — keep retrying next tick.
				m.log.Warn("price feed stale at reveal time, retrying", "window_id", w.ID, "market", w.Market, "waited", waited, "has_fallback_price", snap.Price.IsPositive())
				continue
			}
			m.log.Warn("revealing with stale price after grace period", "window_id", w.ID, "market", w.Market, "waited", waited, "price_age_ms", snap.AgeMs)
		}

		target := snap.Price.Add(snap.Price.Mul(decimal.NewFromInt(w.OffsetBps).Div(decimal.NewFromInt(10000))))
		if err := m.repo.RevealWindow(ctx, w.ID, snap.Price, target, now); err != nil {
			m.log.Error("reveal window", "window_id", w.ID, "err", err)
			continue
		}
		w.Status = models.WindowOpen
		w.TargetPrice = decimal.NewNullDecimal(target)
		m.refreshCache(ctx, w)
	}
	return nil
}

func (m *Manager) lockDueWindows(ctx context.Context, now time.Time) error {
	windows, err := m.repo.DueWindows(ctx, now)
	if err != nil {
		return err
	}
	for _, w := range windows {
		if err := m.repo.LockWindow(ctx, w.ID); err != nil {
			m.log.Error("lock window", "window_id", w.ID, "err", err)
			continue
		}
		w.Status = models.WindowLocked
		m.refreshCache(ctx, w)
		// Refund every still-open (unfilled or partially-filled) order's
		// unmatched remainder now that no more matching can happen — see
		// the plan's unfilled-order risk callout.
		open, err := m.repo.OpenOrdersForWindow(ctx, w.ID)
		if err != nil {
			m.log.Error("list open orders for refund", "window_id", w.ID, "err", err)
			continue
		}
		for _, o := range open {
			remaining := o.Size.Sub(o.FilledSize)
			if remaining.LessThanOrEqual(decimal.Zero) {
				continue
			}
			refund := o.Price.Mul(remaining)
			if o.Side == models.SideNo {
				refund = decimal.NewFromInt(1).Sub(o.Price).Mul(remaining)
			}
			if err := m.client.Unlock(ctx, o.UserID, "BI2XUSD", backendclient.ToRawUnits(refund)); err != nil {
				m.log.Error("unlock unfilled remainder", "order_id", o.ID, "err", err)
				continue
			}
			// Mark the order cancelled now that its remainder is refunded —
			// otherwise it stays "open" forever with no counterparty and no
			// further chance to fill, which is indistinguishable from a
			// genuinely live order to anything reading order status later
			// (e.g. the frontend's order history, a future "your open
			// orders" list).
			newStatus := models.OrderCancelled
			if o.FilledSize.IsPositive() {
				// Partially filled: the matched portion still settles
				// normally, so this isn't a full cancellation, but there's
				// no unmatched remainder left to ever fill either.
				newStatus = models.OrderFilled
			}
			if err := m.repo.CloseRefundedOrder(ctx, o.ID, newStatus); err != nil {
				m.log.Error("mark refunded order terminal", "order_id", o.ID, "err", err)
			}
		}

		// Immediately commit the next round for this market/duration so
		// there is zero gap between one round locking and the next opening.
		if err := m.commitNextWindow(ctx, w.Market, w.Duration, w.EndTime); err != nil {
			m.log.Error("commit next window", "market", w.Market, "duration", w.Duration, "err", err)
		}
	}
	return nil
}

// settleStalenessDeadline bounds how long a locked window waits for a fresh
// price before settling anyway with the last-known (stale) price, mirroring
// revealGracePeriod's identical tradeoff at reveal time above. Without this,
// a price feed outage left settlement stalled indefinitely: positions stayed
// locked, winners unpaid, with no deadline and no alert anywhere in the
// loop — the window would just sit in "locked" forever, `continue`-ing past
// itself every tick until the feed happened to recover. Longer than
// revealGracePeriod (5s) because settlement is the financially final
// action determining every position's payout, so it's worth waiting
// meaningfully longer for a genuinely fresh price before falling back.
const settleStalenessDeadline = 2 * time.Minute

func (m *Manager) settleLockedWindows(ctx context.Context, now time.Time) error {
	windows, err := m.repo.LockedWindows(ctx)
	if err != nil {
		return err
	}
	for _, w := range windows {
		snap := m.prices.Get(ctx, string(w.Market), now.UnixMilli())
		if !snap.Fresh {
			waited := now.Sub(w.EndTime)
			if waited < settleStalenessDeadline || !snap.Price.IsPositive() {
				// Still within the normal settlement window, or there's no
				// price at all to fall back to (feed genuinely never
				// published this asset) — keep retrying next tick, same as
				// openDueCommits' identical wait.
				if waited >= settleStalenessDeadline {
					m.log.Error("settlement stalled: no price at all to fall back to, still retrying",
						"window_id", w.ID, "market", w.Market, "waited", waited)
				}
				continue
			}
			m.log.Warn("settling with stale price after deadline; price feed may be down",
				"window_id", w.ID, "market", w.Market, "waited", waited, "price_age_ms", snap.AgeMs)
		}

		winningSide := models.SideNo
		if snap.Price.GreaterThanOrEqual(w.TargetPrice.Decimal) {
			winningSide = models.SideYes
		}

		if err := m.repo.SettleWindow(ctx, w.ID, snap.Price, now); err != nil {
			m.log.Error("settle window", "window_id", w.ID, "err", err)
			continue
		}
		w.Status = models.WindowSettled
		m.refreshCache(ctx, w)

		positions, err := m.repo.PositionsForWindow(ctx, w.ID)
		if err != nil {
			m.log.Error("list positions for settlement", "window_id", w.ID, "err", err)
			continue
		}
		var totalPaid decimal.Decimal
		for _, p := range positions {
			if p.Side != winningSide || p.Shares.LessThanOrEqual(decimal.Zero) {
				continue
			}
			// Each winning share pays out $1 BI2XUSD notional (the
			// resolved binary outcome), regardless of entry price — profit
			// is the difference between $1 and what the user paid, already
			// realized via the locked cost paid at match time.
			if err := m.client.Credit(ctx, p.UserID, "BI2XUSD", backendclient.ToRawUnits(p.Shares)); err != nil {
				m.log.Error("credit winner", "user_id", p.UserID, "window_id", w.ID, "err", err)
				continue
			}
			totalPaid = totalPaid.Add(p.Shares)
		}

		if err := m.repo.CreateSettlement(ctx, &models.Settlement{
			WindowID: w.ID, WinningSide: winningSide, ResolutionPrice: snap.Price,
			TargetPrice: w.TargetPrice.Decimal, TotalPaidOut: totalPaid,
		}); err != nil {
			m.log.Error("record settlement", "window_id", w.ID, "err", err)
		}
	}
	return nil
}

// broadcastTicks is the live path users actually notice stalling, so it
// never calls Postgres: window state comes from roundcache (kept fresh by
// openDueCommits/lockDueWindows/settleLockedWindows whenever it actually
// changes), and a cache miss just skips that market/duration for one tick
// instead of blocking every market behind a slow database round-trip.
func (m *Manager) broadcastTicks(ctx context.Context, now time.Time) {
	if m.onTick == nil || m.cache == nil {
		return
	}
	for _, market := range allMarkets {
		snap := m.prices.Get(ctx, string(market), now.UnixMilli())
		if !snap.Fresh {
			continue
		}
		for _, duration := range allDurations {
			w, ok := m.cache.Get(ctx, market, duration)
			if !ok || w.Status != models.WindowOpen {
				continue
			}
			remaining := w.EndTime.Sub(now)
			yes := YesPrice(snap.Price, w.TargetPrice, remaining, duration.Window(), duration)
			m.onTick(Tick{
				WindowID: w.ID, Market: market, Duration: duration,
				CurrentPrice: snap.Price, TargetPrice: w.TargetPrice,
				YesPrice: yes, NoPrice: decimal.NewFromInt(1).Sub(yes),
				TimeRemaining: remaining, Status: w.Status,
			})

			if m.history != nil {
				// TTL covers the remaining round time plus a few minutes so
				// the history survives long enough for a "this round closed"
				// screen to still show the final chart after lock/settle.
				ttl := remaining + 5*time.Minute
				point := history.Point{TimestampMs: now.UnixMilli(), CurrentPrice: snap.Price.String(), YesPrice: yes.String()}
				if err := m.history.Append(ctx, w.ID, point, ttl); err != nil {
					m.log.Error("append price history", "window_id", w.ID, "err", err)
				}
			}
		}
	}
}
