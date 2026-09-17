# Known Upgrade: Switch to Trade-Driven Pricing Once Liquidity Justifies It

Status: **not needed yet, current model is correct for now.** This is a
forward-looking plan, not a bug fix — do not implement until the trigger
conditions below are actually met.

## Why the current model is the right choice today

The live yes/no price is currently computed by a formula
(`round.YesPrice` in `internal/round/pricing.go`): a time-decayed function
of how far the live index price is from a hidden random target, not
derived from actual trades on the order book.

This is the correct choice while liquidity is low, for the same reason
every real market (including Polymarket, via its market makers) needs a
reference/model price before real trading volume exists:

- With few real orders, a last-trade or best-bid/ask price would sit stale
  for long stretches between trades, or jump erratically on a single small
  fill.
- It would be trivially manipulable by one user trading against their own
  second account, since there's not enough competing order flow to correct
  a bad print.
- The formula price gives bots (`bots` service) a stable, fair number to
  quote both sides around, which is what lets real user orders find a
  counterparty at all right now — see the fee/payout discussion: your
  settlement is peer-funded (winners are paid from losers' locked funds,
  not platform capital), so a real matched counterparty has to exist for
  every filled order in the first place.

## What "enough liquidity" looks like (trigger conditions)

Revisit this plan when most of the following are true for a sustained
period (say, 1-2 weeks), not just a single busy day:

- Real user order volume (not bot-originated) reliably fills a meaningful
  share of each round's book without needing bot quotes to be involved.
- The order book for each active window has visible depth on both YES and
  NO sides for most of the round's lifetime, not just near open/close.
- Bots are acting more as backstop/liquidity-of-last-resort than as the
  primary counterparty for user fills.
- You have enough trade frequency per window (e.g. multiple real fills per
  minute) that a last-trade or best-bid/ask price would actually update
  often enough to feel live, instead of sitting stale between prints.

## The upgrade itself

Move the *displayed* price (chart + yes/no ticket price) from the formula
to real order-book/trade data, while keeping the formula as the reference
price bots quote around (it doesn't go away — its job narrows).

1. **Add a real-price source to the order book / matcher.**
   Track, per active window: best bid, best ask, and last-trade price for
   both YES and NO sides. This likely lives in or alongside `round.Matcher`,
   wherever fills are currently recorded.

2. **Change `broadcastTicks` to prefer real price over the formula.**
   In `internal/round/manager.go`, `broadcastTicks` currently computes
   `yes := YesPrice(...)` unconditionally. Change the priority to:
   - If there's a recent last-trade price (or a valid best-bid/best-ask
     midpoint) for the window, broadcast that.
   - Otherwise (book empty, e.g. seconds after a round opens), fall back to
     the current formula price.

3. **Keep the formula alive for bot quoting.**
   `bots` should keep quoting both YES and NO around `YesPrice(...)`
   exactly as today — this is what guarantees the book always has a
   reasonable starting depth for real users to trade against, even after
   the displayed price switches to being trade-driven.

4. **Decide a staleness/fallback window for the real price.**
   If the last real trade is, say, more than N seconds old and the book has
   gone empty again (bots paused, liquidity dried up), fall back to the
   formula rather than freezing the displayed price at a stale trade — this
   mirrors how `internal/index.Reader` already treats a stale index price
   as "not fresh" rather than trusting an old number indefinitely.

5. **Frontend chart**: no structural change expected — it already just
   plots whatever `yesPrice`/`currentPrice` the tick carries
   (`internal/history` + `MarketPriceChart`). It will automatically start
   showing real trade-driven movement (jumps, occasional flat stretches,
   sharper moves near expiry) once step 2 ships, matching the Polymarket
   behavior originally being compared against.

6. **Re-verify pricing sanity after the switch**, the same way the
   duration-scaled volatility fix was verified live in
   `PERFORMANCE-FIXES.md`: watch a live window's ticks and confirm the
   price moves in response to real fills, not just BTC spot drift, and that
   it doesn't get stuck at a stale trade price when the book empties out
   again.

## What does NOT change

- Settlement remains peer-funded exactly as today: winners are paid from
  losers' locked funds via `Credit`, platform revenue stays fee-only via
  `SettleFee`. This upgrade only changes what price is *displayed* and
  quoted against — not how money moves at settlement.
- The commit-reveal random target scheme is unaffected — it still decides
  the actual YES/NO settlement outcome (current price vs. target at
  resolution), regardless of what price was displayed along the way.
