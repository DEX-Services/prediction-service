package round

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/models"
)

func dec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func TestYesPrice_AtTargetIsFiftyFifty(t *testing.T) {
	price := YesPrice(dec("50000"), dec("50000"), 3*time.Minute, 5*time.Minute, models.Duration5m)
	got, _ := price.Float64()
	if got < 0.49 || got > 0.51 {
		t.Fatalf("YesPrice at target = %v, want ~0.5", got)
	}
}

func TestYesPrice_AboveTargetFavorsYes(t *testing.T) {
	price := YesPrice(dec("50100"), dec("50000"), 3*time.Minute, 5*time.Minute, models.Duration5m)
	got, _ := price.Float64()
	if got <= 0.5 {
		t.Fatalf("YesPrice above target = %v, want > 0.5 (price above target favors YES)", got)
	}
}

func TestYesPrice_BelowTargetFavorsNo(t *testing.T) {
	price := YesPrice(dec("49900"), dec("50000"), 3*time.Minute, 5*time.Minute, models.Duration5m)
	got, _ := price.Float64()
	if got >= 0.5 {
		t.Fatalf("YesPrice below target = %v, want < 0.5 (price below target favors NO)", got)
	}
}

func TestYesPrice_ClampedToBounds(t *testing.T) {
	// A huge divergence from target should saturate toward the bound, never
	// past it — 0.01/0.99 are the plan's explicit floor/ceiling so a price
	// is never shown as a certainty (0 or 1).
	high := YesPrice(dec("500000"), dec("50000"), 1*time.Second, 5*time.Minute, models.Duration5m)
	low := YesPrice(dec("1"), dec("50000"), 1*time.Second, 5*time.Minute, models.Duration5m)
	if g, _ := high.Float64(); g > 0.99 || g < 0.9 {
		t.Fatalf("YesPrice far above target = %v, want clamped near 0.99", g)
	}
	if g, _ := low.Float64(); g < 0.01 || g > 0.1 {
		t.Fatalf("YesPrice far below target = %v, want clamped near 0.01", g)
	}
}

func TestYesPrice_ConvergesTowardExtremeAsTimeRunsOut(t *testing.T) {
	// Same relative gap from target, but less time remaining (lower
	// uncertainty) should push the price further from 0.5, not closer —
	// this is the exact "sticks at 50/50" regression the duration-scaled
	// volatility fix (see pricing.go's doc comment) was written to prevent.
	early := YesPrice(dec("50050"), dec("50000"), 4*time.Minute, 5*time.Minute, models.Duration5m)
	late := YesPrice(dec("50050"), dec("50000"), 10*time.Second, 5*time.Minute, models.Duration5m)
	earlyVal, _ := early.Float64()
	lateVal, _ := late.Float64()
	if lateVal <= earlyVal {
		t.Fatalf("late-round price (%v) should exceed early-round price (%v) for the same above-target gap", lateVal, earlyVal)
	}
}

func TestYesPrice_ZeroTimeRemainingDoesNotPanic(t *testing.T) {
	price := YesPrice(dec("50100"), dec("50000"), 0, 5*time.Minute, models.Duration5m)
	got, _ := price.Float64()
	if got < 0.01 || got > 0.99 {
		t.Fatalf("YesPrice at zero time remaining = %v, want within [0.01, 0.99]", got)
	}
}

func TestYesPrice_NegativeTimeRemainingClampedToZero(t *testing.T) {
	// A window slightly past its own end_time (scheduler jitter) shouldn't
	// produce a negative sqrt input or otherwise misbehave.
	price := YesPrice(dec("50100"), dec("50000"), -5*time.Second, 5*time.Minute, models.Duration5m)
	got, _ := price.Float64()
	if got < 0.01 || got > 0.99 {
		t.Fatalf("YesPrice with negative time remaining = %v, want within [0.01, 0.99]", got)
	}
}

func TestYesPrice_ZeroWindowLengthFallsBackInsteadOfDividingByZero(t *testing.T) {
	price := YesPrice(dec("50100"), dec("50000"), 1*time.Minute, 0, models.Duration5m)
	got, _ := price.Float64()
	if got < 0.01 || got > 0.99 {
		t.Fatalf("YesPrice with zero windowLength = %v, want within [0.01, 0.99] (no NaN/Inf)", got)
	}
}

func TestDurationVolatility_15mWiderThan5m(t *testing.T) {
	// 15m's offset range (15-40bps, midpoint 27.5) is nearly 3x 5m's
	// (5-15bps, midpoint 10) — see pricing.go's doc comment on why this
	// scaling exists at all (a shared flat volatility made 15m rounds stick
	// near 50/50 for most of the window).
	v5 := durationVolatility(models.Duration5m)
	v15 := durationVolatility(models.Duration15m)
	if v15 <= v5 {
		t.Fatalf("durationVolatility(15m) = %v, want > durationVolatility(5m) = %v", v15, v5)
	}
}
