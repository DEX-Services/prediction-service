package round

import (
	"math"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/models"
)

// baseVolatility tunes how quickly the YES probability saturates toward 0/1
// as price diverges from target, expressed as a fraction of price (e.g.
// 0.0015 = 0.15%). It is scaled per-duration (see durationVolatility) to the
// midpoint of that duration's random target-offset range in
// commitreveal.go, rather than shared as one flat constant — 15m rounds draw
// a 15-40bps offset (midpoint ~27.5bps) vs 5m's 5-15bps (midpoint ~10bps),
// nearly 3x wider. With one shared baseVolatility tuned for 5m, a 15m
// round's uncertainty was too large relative to its own target gap, so
// tanh(edge) stayed near 0 (yes price stuck near 50/50) for most of the
// round, only moving once uncertainty decayed enough near expiry — the
// "sticks at 50-50 for 15 minutes" symptom. Scaling baseVolatility to each
// duration's own offset midpoint keeps the edge/uncertainty ratio, and thus
// the price's responsiveness to a real price move, consistent across both
// durations.
const baseVolatility = 0.0015

// referenceOffsetBps is the offset-range midpoint baseVolatility above was
// tuned against (5m's (5+15)/2), so durationVolatility scales relative to it.
const referenceOffsetBps = 10.0

func durationVolatility(d models.Duration) float64 {
	lo, hi := offsetRangeBps(d)
	midpoint := float64(lo+hi) / 2
	return baseVolatility * (midpoint / referenceOffsetBps)
}

// YesPrice implements the plan's time-decayed logistic pricing formula:
//
//	uncertainty(t) = duration_volatility * sqrt(time_remaining / window_length)
//	edge           = ((current_price - target_price) / target_price) / uncertainty(t)
//	yes_price      = clamp(0.5 + 0.48*tanh(edge), 0.01, 0.99)
//
// The price gap is normalized as a fraction of the target price before
// comparing against uncertainty (also a fraction) — comparing a raw dollar
// gap against a dimensionless uncertainty made every BTC/ETH round saturate
// to 1%/99% within moments of opening, since even a few dollars' movement
// is astronomically larger than baseVolatility taken as an absolute number.
// uncertainty shrinks as the round approaches resolution, so the same
// fractional gap from target pushes the probability further toward its
// extreme the closer the round gets to expiry.
func YesPrice(currentPrice, targetPrice decimal.Decimal, timeRemaining, windowLength time.Duration, duration models.Duration) decimal.Decimal {
	if windowLength <= 0 {
		windowLength = time.Minute
	}
	if timeRemaining < 0 {
		timeRemaining = 0
	}
	frac := float64(timeRemaining) / float64(windowLength)
	uncertainty := durationVolatility(duration) * math.Sqrt(frac)
	if uncertainty < 1e-6 {
		uncertainty = 1e-6
	}

	target, _ := targetPrice.Float64()
	if target == 0 {
		target = 1
	}
	diff, _ := currentPrice.Sub(targetPrice).Float64()
	relativeDiff := diff / target
	edge := relativeDiff / uncertainty

	yes := 0.5 + 0.48*math.Tanh(edge)
	if yes < 0.01 {
		yes = 0.01
	}
	if yes > 0.99 {
		yes = 0.99
	}
	return decimal.NewFromFloat(yes).Round(4)
}
