// Package backendclient calls Dex-Backend's dedicated prediction-wallet
// endpoints (/internal/prediction-wallet/*) and its generic
// /internal/balance/fee and /internal/user/ensure endpoints — Phase 4 of
// ~/.claude/plans/wallet-separation.md. This service's YES/NO order fills
// and round payouts move real BI2XUSD, but through prediction-service's
// own dedicated Postgres wallet (prediction_wallet_balances) rather than
// the shared main user_balances pool every other feature draws from — a
// user must explicitly fund this wallet (POST /wallet/prediction/fund on
// Dex-Backend) before this service can lock/debit against it, the same
// funding-transfer pattern staking and P2P already use.
package backendclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// RawUnitScale is the number of decimals Dex-Backend's Postgres wallet
// columns use for raw fixed-point integer storage (matches USDC's 6
// on-chain decimals, e.g. 40000000 = $40). Must match matching-engine's own
// RawUnitScale exactly — both write into the same platform-wide convention.
const RawUnitScale = 6

// ToRawUnits converts a decimal BI2XUSD amount to the raw integer string
// Dex-Backend expects.
func ToRawUnits(amount decimal.Decimal) string {
	return amount.Shift(RawUnitScale).Truncate(0).String()
}

// Client calls Dex-Backend's internal prediction-wallet endpoints. A
// nil/zero-value Client (created when DEX_BACKEND_URL or
// DEX_BACKEND_ENGINE_SECRET is unset) no-ops every call so the service
// fails loudly at startup instead of silently trading with funds that
// never actually move — see New's check.
type Client struct {
	baseURL string
	secret  string
	http    *http.Client
}

// New builds a Client from DEX_BACKEND_URL / DEX_BACKEND_ENGINE_SECRET env
// vars. Unlike matching-engine's backendclient (where a disabled client is a
// legitimate dev-mode fallback), this service has no useful in-memory-only
// mode — an order book that can't actually move BI2XUSD shouldn't accept
// real orders — so New returns an error instead of a silently-disabled
// client when either var is missing.
func New() (*Client, error) {
	base := os.Getenv("DEX_BACKEND_URL")
	secret := os.Getenv("DEX_BACKEND_ENGINE_SECRET")
	if base == "" || secret == "" {
		return nil, fmt.Errorf("DEX_BACKEND_URL and DEX_BACKEND_ENGINE_SECRET must both be set")
	}
	return &Client{
		baseURL: base,
		secret:  secret,
		http: &http.Client{
			Timeout: 20 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}, nil
}

// NewForTest builds a Client pointed at an arbitrary base URL/secret/http
// client, for tests (e.g. internal/round) that need to stub Dex-Backend's
// prediction-wallet endpoints without depending on unexported fields or env
// vars — mirrors matching-engine's engineclient.NewForTest.
func NewForTest(baseURL, secret string, httpClient *http.Client) *Client {
	return &Client{baseURL: baseURL, secret: secret, http: httpClient}
}

type ensureUserReq struct {
	UserID string `json:"userId"`
}

// EnsureUser calls POST /internal/user/ensure — guarantees a users row
// exists for userID before this service locks/credits real balances
// against it. Dex-Backend only creates that row automatically at wallet
// login (Server.Login's FindOrCreate); a valid JWT can otherwise exist
// without one (e.g. the admin panel's session, which never calls
// FindOrCreate), and locking funds for a userID with no users row fails
// the prediction wallet's foreign key. Idempotent — safe to call on every
// order. Unchanged by Phase 4: this endpoint has nothing to do with which
// wallet a balance lands in.
func (c *Client) EnsureUser(ctx context.Context, userID string) error {
	body, err := json.Marshal(ensureUserReq{UserID: userID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/user/ensure", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("backendclient /internal/user/ensure: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendclient /internal/user/ensure: status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

type predictionWalletReq struct {
	UserID      string `json:"userId"`
	PositionRef string `json:"positionRef"`
	Amount      string `json:"amount"`
}

// Lock calls POST /internal/prediction-wallet/lock — holds amount of
// BI2XUSD against userID's prediction wallet available balance without yet
// debiting it, for an order resting on the book. positionRef is a
// free-text tag (this service's own order id) carried into the wallet's
// audit trail (prediction_wallet_entries), purely for operational
// traceability — Dex-Backend never interprets it.
func (c *Client) Lock(ctx context.Context, userID, positionRef, amount string) error {
	return c.call(ctx, "/internal/prediction-wallet/lock", userID, positionRef, amount, "")
}

// LockIdempotent is Lock with an Idempotency-Key header attached; see
// CreditIdempotent's doc comment for the dedup guarantee.
func (c *Client) LockIdempotent(ctx context.Context, userID, positionRef, amount, idempotencyKey string) error {
	return c.call(ctx, "/internal/prediction-wallet/lock", userID, positionRef, amount, idempotencyKey)
}

// Unlock calls POST /internal/prediction-wallet/unlock — releases a hold
// taken by Lock, e.g. when an order is cancelled or a round refunds an
// unfilled order.
func (c *Client) Unlock(ctx context.Context, userID, positionRef, amount string) error {
	return c.call(ctx, "/internal/prediction-wallet/unlock", userID, positionRef, amount, "")
}

// UnlockIdempotent is Unlock with an Idempotency-Key header attached.
func (c *Client) UnlockIdempotent(ctx context.Context, userID, positionRef, amount, idempotencyKey string) error {
	return c.call(ctx, "/internal/prediction-wallet/unlock", userID, positionRef, amount, idempotencyKey)
}

// Debit calls POST /internal/prediction-wallet/debit — consumes amount
// from userID's locked (reserved) prediction wallet balance, e.g. a match
// consuming part of an order's lock at execution price plus fee. Replaces
// the old design's negative-amount Credit call (this wallet's Debit only
// ever draws from reserved_raw, which a signed Credit against the old
// shared main-wallet balance had no equivalent concept of).
func (c *Client) Debit(ctx context.Context, userID, positionRef, amount string) error {
	return c.call(ctx, "/internal/prediction-wallet/debit", userID, positionRef, amount, "")
}

// DebitIdempotent is Debit with an Idempotency-Key header attached.
func (c *Client) DebitIdempotent(ctx context.Context, userID, positionRef, amount, idempotencyKey string) error {
	return c.call(ctx, "/internal/prediction-wallet/debit", userID, positionRef, amount, idempotencyKey)
}

// Credit calls POST /internal/prediction-wallet/credit — adds amount to
// userID's available prediction wallet balance, e.g. a round settlement's
// payout to a winning position. Always positive — a round payout is new
// money landing in the wallet, never a debit (see Debit above for
// consuming a lock).
func (c *Client) Credit(ctx context.Context, userID, positionRef, amount string) error {
	return c.call(ctx, "/internal/prediction-wallet/credit", userID, positionRef, amount, "")
}

// CreditIdempotent is Credit with an Idempotency-Key header attached, for
// callers that may retry the same logical operation (M7/PRED-H2's
// reconciliation sweep). Dex-Backend dedupes on (kind, key): a retry with
// the same key is a safe no-op even if the original call already landed —
// see PredictionWalletRepo.Credit/Lock/Unlock/Debit in
// Dex-Backend/internal/repo/prediction_wallet.go.
func (c *Client) CreditIdempotent(ctx context.Context, userID, positionRef, amount, idempotencyKey string) error {
	return c.call(ctx, "/internal/prediction-wallet/credit", userID, positionRef, amount, idempotencyKey)
}

type feeSettleReq struct {
	UserID   string `json:"userId"`
	Asset    string `json:"asset"`
	Amount   string `json:"amount"`
	Category string `json:"category"`
}

// SettleFee calls POST /internal/balance/fee with category "prediction" —
// routes a maker/taker fee (already deducted from the paying user's fill
// via Debit above) to their referral/affiliate beneficiary (if any) plus
// the platform treasury for the remainder, tagged so prediction-market fee
// revenue is reportable separately from spot/futures fee revenue. This is
// deliberately NOT one of the new prediction-wallet endpoints: fee revenue
// routes into platform_treasury_entries/referral balances, never into (or
// out of) a user's own prediction_wallet_balances row, so it has nothing
// to do with which wallet this service's trading funds live in. Requires
// Dex-Backend's platform_treasury_entries.category CHECK constraint and
// InternalSettleFee handler to accept "prediction" — see
// ensureTreasuryEntryPredictionCategory in Dex-Backend/internal/db/db.go.
func (c *Client) SettleFee(ctx context.Context, userID, asset, amount string) error {
	return c.settleFee(ctx, userID, asset, amount, "")
}

// SettleFeeIdempotent is SettleFee with an Idempotency-Key header attached;
// see CreditIdempotent's doc comment for the dedup guarantee this gives.
func (c *Client) SettleFeeIdempotent(ctx context.Context, userID, asset, amount, idempotencyKey string) error {
	return c.settleFee(ctx, userID, asset, amount, idempotencyKey)
}

func (c *Client) settleFee(ctx context.Context, userID, asset, amount, idempotencyKey string) error {
	body, err := json.Marshal(feeSettleReq{UserID: userID, Asset: asset, Amount: amount, Category: "prediction"})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/balance/fee", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("backendclient /internal/balance/fee: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendclient /internal/balance/fee: status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

// AvailableBalance calls GET /internal/prediction-wallet/available,
// returning the raw available (total minus locked) BI2XUSD amount for
// userID's prediction wallet — used before accepting an order to give the
// user a fast, clear rejection instead of letting Lock fail after the
// order was already partially processed.
func (c *Client) AvailableBalance(ctx context.Context, userID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/prediction-wallet/available", nil)
	if err != nil {
		return "", err
	}
	q := req.URL.Query()
	q.Set("userId", userID)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-Engine-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("backendclient /internal/prediction-wallet/available: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("backendclient /internal/prediction-wallet/available: status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var result struct {
		Available string `json:"available"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("backendclient /internal/prediction-wallet/available: decode response: %w", err)
	}
	return result.Available, nil
}

func (c *Client) call(ctx context.Context, path, userID, positionRef, amount, idempotencyKey string) error {
	body, err := json.Marshal(predictionWalletReq{UserID: userID, PositionRef: positionRef, Amount: amount})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("backendclient %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendclient %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

var _ = slog.Default // keep slog imported for future logging without churn
