package round

import (
	"math"
	"time"

	"github.com/shopspring/decimal"
)

// baseVolatility tunes how quickly the YES probability saturates toward 0/1
// as price diverges from target, expressed as a fraction of price (e.g.
// 0.0015 = 0.15%) — matches the plan's worked examples and the same
// magnitude as the random target offset range in commitreveal.go, so a
// round's full offset range maps to a meaningful (not instantly-saturated)
// spread of contract prices over the round's lifetime.
const baseVolatility = 0.0015

// YesPrice implements the plan's time-decayed logistic pricing formula:
//
//	uncertainty(t) = base_volatility * sqrt(time_remaining / window_length)
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
func YesPrice(currentPrice, targetPrice decimal.Decimal, timeRemaining, windowLength time.Duration) decimal.Decimal {
	if windowLength <= 0 {
		windowLength = time.Minute
	}
	if timeRemaining < 0 {
		timeRemaining = 0
	}
	frac := float64(timeRemaining) / float64(windowLength)
	uncertainty := baseVolatility * math.Sqrt(frac)
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
