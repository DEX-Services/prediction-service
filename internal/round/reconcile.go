package round

import (
	"context"
	"fmt"
)

// pendingFillGracePeriod is how long a pending-fill row is left alone before
// the sweep retries it. PlaceOrder settles its own fills immediately after
// creating them, so any row younger than this is very likely still being
// worked by the request that created it, not actually stuck.
const pendingFillGracePeriod = tickStepTimeout * 4

// reconcilePendingFills retries matches whose settlement (the Dex-Backend
// HTTP calls plus position/fill bookkeeping in settleFillByPlan) never
// completed — because the process died between the order-matching
// transaction committing and settlement finishing, or because a backend
// call failed and PlaceOrder gave up and returned an error to its caller
// without retrying itself (M7/PRED-H2). Each retry reuses the pending
// fill's stored idempotency key, so it's the same logical operation being
// resent, not a new one.
func (m *Manager) reconcilePendingFills(ctx context.Context) error {
	stale, err := m.repo.StalePendingFills(ctx, pendingFillGracePeriod)
	if err != nil {
		return fmt.Errorf("list stale pending fills: %w", err)
	}
	for _, pf := range stale {
		if err := m.matcher.settleFillByPlan(ctx, pf); err != nil {
			m.log.Error("reconcile pending fill failed, will retry next tick",
				"pending_fill_id", pf.ID, "window_id", pf.WindowID, "attempts", pf.Attempts+1, "err", err)
			if recErr := m.repo.RecordPendingFillAttempt(ctx, pf.ID, err.Error()); recErr != nil {
				m.log.Error("record pending fill attempt", "pending_fill_id", pf.ID, "err", recErr)
			}
			continue
		}
		m.log.Warn("reconciled a pending fill that had been stuck",
			"pending_fill_id", pf.ID, "window_id", pf.WindowID,
			"maker_order_id", pf.MakerOrderID, "taker_order_id", pf.TakerOrderID, "prior_attempts", pf.Attempts)
	}
	return nil
}
