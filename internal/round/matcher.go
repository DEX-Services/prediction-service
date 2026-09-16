package round

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/backendclient"
	"github.com/dex/prediction-service/internal/models"
	"github.com/dex/prediction-service/internal/repo"
)

// Fee rates per the plan: resting (maker) orders pay less than the
// order that crosses the book and takes liquidity (taker).
var (
	makerFeeRate = decimal.NewFromFloat(0.00015) // 0.015%
	takerFeeRate = decimal.NewFromFloat(0.00045) // 0.045%
)

// Matcher runs price-time-priority matching for one window's YES/NO book.
// There is no market maker — every fill is between two real user orders, so
// an order can rest partially or fully unfilled if there's no opposing
// liquidity at an acceptable price (see the plan's unfilled-order risk
// callout; unfilled remainders are refunded at window lock, see round.go).
type Matcher struct {
	repo   *repo.Repo
	client *backendclient.Client
}

func NewMatcher(r *repo.Repo, c *backendclient.Client) *Matcher {
	return &Matcher{repo: r, client: c}
}

// opposite returns the other side of a binary market.
func opposite(s models.OrderSide) models.OrderSide {
	if s == models.SideYes {
		return models.SideNo
	}
	return models.SideYes
}

// PlaceOrder locks the order's notional cost against the user's real balance,
// then matches it price-time-priority against the resting opposite-side book,
// crediting/debiting real balances and applying maker/taker fees per fill.
// Any unfilled remainder rests on the book as an open order.
//
// A YES order at price p and a NO order at price q are compatible when
// p + q >= 1 (i.e. q >= 1-p): the resting order's price is what the trade
// executes at, giving price-time priority to whoever was there first.
func (m *Matcher) PlaceOrder(ctx context.Context, o *models.Order) (int64, []*models.Fill, error) {
	cost := o.Price.Mul(o.Size)
	if o.Side == models.SideNo {
		cost = decimal.NewFromInt(1).Sub(o.Price).Mul(o.Size)
	}
	rawCost := backendclient.ToRawUnits(cost)
	if err := m.client.Lock(ctx, o.UserID, "BI2XUSD", rawCost); err != nil {
		return 0, nil, fmt.Errorf("lock order cost: %w", err)
	}

	orderID, err := m.repo.CreateOrder(ctx, o)
	if err != nil {
		_ = m.client.Unlock(ctx, o.UserID, "BI2XUSD", rawCost)
		return 0, nil, fmt.Errorf("create order: %w", err)
	}
	o.ID = orderID

	var fills []*models.Fill
	err = m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		book, err := m.repo.OpposingBook(ctx, o.WindowID, opposite(o.Side), tx)
		if err != nil {
			return err
		}
		remaining := o.Size
		for _, resting := range book {
			if remaining.IsZero() {
				break
			}
			restingRemaining := resting.Size.Sub(resting.FilledSize)
			if restingRemaining.LessThanOrEqual(decimal.Zero) {
				continue
			}

			// Compatibility check: incoming YES@p vs resting NO@q needs p+q>=1;
			// incoming NO@p vs resting YES@q needs p+q>=1 too (symmetric).
			var sumOK bool
			if o.Side == models.SideYes {
				sumOK = o.Price.Add(resting.Price).GreaterThanOrEqual(decimal.NewFromInt(1))
			} else {
				sumOK = resting.Price.Add(o.Price).GreaterThanOrEqual(decimal.NewFromInt(1))
			}
			if !sumOK {
				continue
			}

			fillSize := decimal.Min(remaining, restingRemaining)
			// Trade executes at the resting order's price (maker sets price).
			execPrice := resting.Price

			if err := m.settleFill(ctx, tx, o, resting, execPrice, fillSize); err != nil {
				return err
			}

			f := &models.Fill{
				WindowID:     o.WindowID,
				MakerOrderID: resting.ID,
				TakerOrderID: o.ID,
				Price:        execPrice,
				Size:         fillSize,
			}
			fills = append(fills, f)

			remaining = remaining.Sub(fillSize)

			restingNewFilled := resting.FilledSize.Add(fillSize)
			restingStatus := models.OrderOpen
			if restingNewFilled.GreaterThanOrEqual(resting.Size) {
				restingStatus = models.OrderFilled
			}
			if err := m.repo.FillOrder(ctx, tx, resting.ID, fillSize, restingStatus); err != nil {
				return err
			}
			resting.FilledSize = restingNewFilled
		}

		takerNewFilled := o.Size.Sub(remaining)
		takerStatus := models.OrderOpen
		if remaining.IsZero() {
			takerStatus = models.OrderFilled
		}
		if err := m.repo.FillOrder(ctx, tx, o.ID, takerNewFilled, takerStatus); err != nil {
			return err
		}
		o.FilledSize = takerNewFilled
		o.Status = takerStatus
		return nil
	})
	if err != nil {
		return orderID, nil, fmt.Errorf("match order: %w", err)
	}
	return orderID, fills, nil
}

// settleFill moves real BI2XUSD for one match: consumes both sides' locked
// funds via signed Credit calls, records fee revenue via SettleFee, and
// updates both users' positions. execPrice is the YES-probability price the
// resting (maker) order set; the taker's economics are computed from the
// complementary side.
func (m *Matcher) settleFill(ctx context.Context, tx pgx.Tx, taker, maker *models.Order, execPrice, size decimal.Decimal) error {
	// Cost each side actually pays for this fill, in their own side's terms.
	makerCost := execPrice.Mul(size)
	takerCost := decimal.NewFromInt(1).Sub(execPrice).Mul(size)
	if maker.Side == models.SideNo {
		makerCost, takerCost = takerCost, makerCost
	}

	makerFee := makerCost.Mul(makerFeeRate)
	takerFee := takerCost.Mul(takerFeeRate)

	// Consume the locked hold: the lock was taken at order-placement price,
	// which may differ slightly from execPrice for a maker whose resting
	// price improved after being crossed; both sides only ever pay execPrice
	// economics here, debited via a negative Credit against their lock.
	if err := m.client.Credit(ctx, maker.UserID, "BI2XUSD", "-"+backendclient.ToRawUnits(makerCost.Add(makerFee))); err != nil {
		return fmt.Errorf("debit maker: %w", err)
	}
	if err := m.client.Credit(ctx, taker.UserID, "BI2XUSD", "-"+backendclient.ToRawUnits(takerCost.Add(takerFee))); err != nil {
		return fmt.Errorf("debit taker: %w", err)
	}
	if err := m.client.SettleFee(ctx, maker.UserID, "BI2XUSD", backendclient.ToRawUnits(makerFee)); err != nil {
		return fmt.Errorf("settle maker fee: %w", err)
	}
	if err := m.client.SettleFee(ctx, taker.UserID, "BI2XUSD", backendclient.ToRawUnits(takerFee)); err != nil {
		return fmt.Errorf("settle taker fee: %w", err)
	}

	if err := m.repo.UpsertPosition(ctx, tx, maker.WindowID, maker.UserID, maker.Side, size, execPrice); err != nil {
		return err
	}
	takerPrice := execPrice
	if taker.Side == models.SideNo {
		takerPrice = decimal.NewFromInt(1).Sub(execPrice)
	}
	if err := m.repo.UpsertPosition(ctx, tx, taker.WindowID, taker.UserID, taker.Side, size, takerPrice); err != nil {
		return err
	}
	return m.repo.CreateFill(ctx, tx, &models.Fill{
		WindowID: maker.WindowID, MakerOrderID: maker.ID, TakerOrderID: taker.ID,
		Price: execPrice, Size: size, MakerFee: makerFee, TakerFee: takerFee,
	})
}

// CancelOrder cancels a resting order and unlocks its unfilled remainder.
func (m *Matcher) CancelOrder(ctx context.Context, orderID int64, userID string, side models.OrderSide, price decimal.Decimal) error {
	remaining, err := m.repo.CancelUserOrder(ctx, orderID, userID)
	if err != nil {
		return err
	}
	refund := price.Mul(remaining)
	if side == models.SideNo {
		refund = decimal.NewFromInt(1).Sub(price).Mul(remaining)
	}
	return m.client.Unlock(ctx, userID, "BI2XUSD", backendclient.ToRawUnits(refund))
}
