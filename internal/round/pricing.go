package round

import (
	"math"
	"time"

	"github.com/shopspring/decimal"
)

// baseVolatility tunes how quickly the YES probability saturates toward 0/1
// as price diverges from target — matches the plan's worked examples.
const baseVolatility = 0.35

// YesPrice implements the plan's time-decayed logistic pricing formula:
//
//	uncertainty(t) = base_volatility * sqrt(time_remaining / window_length)
//	edge           = (current_price - target_price) / uncertainty(t)
//	yes_price      = clamp(0.5 + 0.48*tanh(edge), 0.01, 0.99)
//
// uncertainty shrinks as the round approaches resolution, so the same price
// gap from target pushes the probability further toward its extreme the
// closer the round gets to expiry.
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

	diff, _ := currentPrice.Sub(targetPrice).Float64()
	edge := diff / uncertainty

	yes := 0.5 + 0.48*math.Tanh(edge)
	if yes < 0.01 {
		yes = 0.01
	}
	if yes > 0.99 {
		yes = 0.99
	}
	return decimal.NewFromFloat(yes).Round(4)
}
