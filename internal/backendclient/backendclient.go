// Package backendclient calls Dex-Backend's /internal/balance/* endpoints —
// the same internal API matching-engine already uses (see that repo's
// internal/backendclient package, which this file mirrors closely on
// purpose) — so this service's YES/NO order fills and round payouts move
// real BI2XUSD in the platform's actual wallet balances, not a private copy
// of them.
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

// RawUnitScale is the number of decimals Dex-Backend's Postgres user_balances
// columns use for raw fixed-point integer storage (matches USDC's 6 on-chain
// decimals, e.g. 40000000 = $40). Must match matching-engine's own
// RawUnitScale exactly — both write into the same columns.
const RawUnitScale = 6

// ToRawUnits converts a decimal BI2XUSD amount to the raw integer string
// Dex-Backend expects.
func ToRawUnits(amount decimal.Decimal) string {
	return amount.Shift(RawUnitScale).Truncate(0).String()
}

// Client calls Dex-Backend's internal balance endpoints. A nil/zero-value
// Client (created when DEX_BACKEND_URL or DEX_BACKEND_ENGINE_SECRET is
// unset) no-ops every call so the service fails loudly at startup instead of
// silently trading with funds that never actually move — see New's check.
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

type ensureUserReq struct {
	UserID string `json:"userId"`
}

// EnsureUser calls POST /internal/user/ensure — guarantees a users row
// exists for userID before this service locks/credits real balances
// against it. Dex-Backend only creates that row automatically at wallet
// login (Server.Login's FindOrCreate); a valid JWT can otherwise exist
// without one (e.g. the admin panel's session, which never calls
// FindOrCreate), and locking funds for a userID with no users row fails
// user_balances' foreign key. Idempotent — safe to call on every order.
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

type balanceReq struct {
	UserID string `json:"userId"`
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
}

// Lock calls POST /internal/balance/lock — holds amount of asset against
// userID's real Postgres balance without yet debiting it, for an order
// resting on the book.
func (c *Client) Lock(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/lock", userID, asset, amount)
}

// Unlock calls POST /internal/balance/unlock — releases a hold taken by
// Lock, e.g. when an order is cancelled or a round refunds an unfilled
// order.
func (c *Client) Unlock(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/unlock", userID, asset, amount)
}

// Credit calls POST /internal/balance/credit — a generic signed adjustment
// to userID's real balance. Positive credits (a round payout); negative
// debits (consuming a lock at match/settlement time). This is the primary
// call this service makes: placing an order locks funds via Lock, a match
// consumes the matched portion via a negative Credit (mirroring how the
// hold is actually spent), and round settlement pays winners via a positive
// Credit.
func (c *Client) Credit(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/credit", userID, asset, amount)
}

type feeSettleReq struct {
	UserID   string `json:"userId"`
	Asset    string `json:"asset"`
	Amount   string `json:"amount"`
	Category string `json:"category"`
}

// SettleFee calls POST /internal/balance/fee with category "prediction" —
// routes a maker/taker fee (already deducted from the paying user's fill)
// to their referral/affiliate beneficiary (if any) plus the platform
// treasury for the remainder, tagged so prediction-market fee revenue is
// reportable separately from spot/futures fee revenue. Requires
// Dex-Backend's platform_treasury_entries.category CHECK constraint and
// InternalSettleFee handler to accept "prediction" — see
// ensureTreasuryEntryPredictionCategory in Dex-Backend/internal/db/db.go.
func (c *Client) SettleFee(ctx context.Context, userID, asset, amount string) error {
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

// AvailableBalance calls GET /internal/balance/available, returning the raw
// available (total minus locked) amount of asset for userID — used before
// accepting an order to give the user a fast, clear rejection instead of
// letting Lock fail after the order was already partially processed.
func (c *Client) AvailableBalance(ctx context.Context, userID, asset string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/balance/available", nil)
	if err != nil {
		return "", err
	}
	q := req.URL.Query()
	q.Set("userId", userID)
	q.Set("asset", asset)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-Engine-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("backendclient /internal/balance/available: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("backendclient /internal/balance/available: status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var result struct {
		Available string `json:"available"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("backendclient /internal/balance/available: decode response: %w", err)
	}
	return result.Available, nil
}

func (c *Client) call(ctx context.Context, path, userID, asset, amount string) error {
	body, err := json.Marshal(balanceReq{UserID: userID, Asset: asset, Amount: amount})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)
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
