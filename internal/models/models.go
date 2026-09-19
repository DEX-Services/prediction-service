// Package models holds the shared struct definitions for prediction rounds,
// orders, positions, and settlements used across the db/repo/round/api
// layers of this service.
package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Market identifies one of the 6 always-rolling round streams.
type Market string

const (
	MarketBTC Market = "BTC"
	MarketETH Market = "ETH"
	MarketSOL Market = "SOL"
)

// Duration identifies a round length.
type Duration string

const (
	Duration5m  Duration = "5m"
	Duration15m Duration = "15m"
)

func (d Duration) Window() time.Duration {
	switch d {
	case Duration5m:
		return 5 * time.Minute
	case Duration15m:
		return 15 * time.Minute
	}
	return 0
}

// WindowStatus tracks a round's lifecycle.
type WindowStatus string

const (
	// WindowCommitted: offset+salt hash published, price not yet locked in;
	// orders are NOT yet accepted (opening price/target aren't final until
	// the round actually starts).
	WindowCommitted WindowStatus = "committed"
	// WindowOpen: round is live, opening price and target price are fixed
	// and revealed, orders are being accepted and matched.
	WindowOpen WindowStatus = "open"
	// WindowLocked: window's end time has passed, no more new orders are
	// accepted, waiting on final resolution price from the feed.
	WindowLocked WindowStatus = "locked"
	// WindowSettled: resolution price recorded, winners paid, fully done.
	WindowSettled WindowStatus = "settled"
)

// Window is one round of one market/duration pair — e.g. "BTC 5m round
// starting at 14:32:00".
type Window struct {
	ID       int64
	Market   Market
	Duration Duration

	// CommitHash = sha256(OffsetBps || Salt), published at commit time
	// (round N-1's open) before OffsetBps/Salt are known publicly, so no one
	// -- including an insider -- can see the random target before trading
	// opens. Salt/OffsetBps are stored in the database from the moment the
	// window is created (so a restart can't lose them) but must never be
	// exposed by the API before RevealedAt is set — that's what actually
	// enforces the commit-reveal guarantee, not withholding them in storage.
	CommitHash string
	Salt       string
	OffsetBps  int64

	// OpeningPrice/TargetPrice are NULL until revealed, ResolutionPrice is
	// NULL until settled.
	OpeningPrice    decimal.NullDecimal
	TargetPrice     decimal.NullDecimal
	ResolutionPrice decimal.NullDecimal

	Status WindowStatus

	CommitTime time.Time // when the hash was published (previous round's start)
	StartTime  time.Time // when trading opens or opened
	EndTime    time.Time // when trading locks / round resolves
	RevealedAt *time.Time
	SettledAt  *time.Time
}

// OrderSide is which outcome the order backs.
type OrderSide string

const (
	SideYes OrderSide = "yes" // price will be AT OR ABOVE target at resolution
	SideNo  OrderSide = "no"
)

// OrderStatus tracks an order's matching lifecycle.
type OrderStatus string

const (
	OrderOpen      OrderStatus = "open"      // resting on the book, partially or fully unfilled
	OrderFilled    OrderStatus = "filled"    // fully matched
	OrderCancelled OrderStatus = "cancelled" // cancelled by user or refunded at round lock if unfilled
)

// Order is a single YES/NO limit order in one window's book.
type Order struct {
	ID       int64
	WindowID int64
	UserID   string
	Side     OrderSide

	// Price is the order's limit price in the [0.01, 0.99] YES-probability
	// space (a NO order's economics are derived as 1-Price when matched
	// against a YES order at the same book price).
	Price decimal.Decimal
	// Size is the order's total requested contract size in BI2XUSD notional.
	Size       decimal.Decimal
	FilledSize decimal.Decimal

	Status    OrderStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Position is one user's net accumulated YES or NO exposure in one window,
// built up from one or more fills.
type Position struct {
	ID       int64
	WindowID int64
	UserID   string
	Side     OrderSide

	Shares    decimal.Decimal // total contract size held
	AvgPrice  decimal.Decimal // size-weighted average entry price
	Realized  decimal.Decimal // realized P/L from any early exits already settled
	PaidOut   bool            // settlement payout already Credit()-ed (idempotency guard)
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Fill records one match between a resting (maker) and incoming (taker)
// order — the audit trail backing both orders' FilledSize and both users'
// Position updates, and the source of truth for maker/taker fee amounts.
type Fill struct {
	ID           int64
	WindowID     int64
	MakerOrderID int64
	TakerOrderID int64
	Price        decimal.Decimal
	Size         decimal.Decimal
	MakerFee     decimal.Decimal
	TakerFee     decimal.Decimal
	CreatedAt    time.Time
}

// PendingFill is a not-yet-settled match: written durably before any
// Dex-Backend HTTP call so a crash or failed call between "orders marked
// filled" and "positions/fees recorded" can be found and retried instead of
// silently leaving that fill unsettled forever (M7/PRED-H2).
type PendingFill struct {
	ID             int64
	WindowID       int64
	MakerOrderID   int64
	TakerOrderID   int64
	MakerUserID    string
	TakerUserID    string
	MakerSide      OrderSide
	TakerSide      OrderSide
	ExecPrice      decimal.Decimal
	Size           decimal.Decimal
	IdempotencyKey string
	Attempts       int
	CreatedAt      time.Time
}

// Settlement is the permanent resolution record for one window — target vs.
// resolution price, the winning side, and the total paid out.
type Settlement struct {
	ID              int64
	WindowID        int64
	WinningSide     OrderSide
	ResolutionPrice decimal.Decimal
	TargetPrice     decimal.Decimal
	TotalPaidOut    decimal.Decimal
	SettledAt       time.Time
}
