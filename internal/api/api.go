// Package api exposes this service's HTTP endpoints: placing/cancelling
// orders, listing a user's positions/order history, reading active window
// state, and the WebSocket tick stream.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/shopspring/decimal"

	"github.com/dex/prediction-service/internal/auth"
	"github.com/dex/prediction-service/internal/history"
	"github.com/dex/prediction-service/internal/models"
	"github.com/dex/prediction-service/internal/repo"
	"github.com/dex/prediction-service/internal/round"
	"github.com/dex/prediction-service/internal/wshub"
)

const sessionCookie = "dex_session"

type Server struct {
	repo    *repo.Repo
	matcher *round.Matcher
	jwt     *auth.JWTIssuer
	hub     *wshub.Hub
	history *history.Store
	log     *slog.Logger
}

func NewServer(r *repo.Repo, matcher *round.Matcher, jwt *auth.JWTIssuer, hub *wshub.Hub, hist *history.Store, log *slog.Logger) *Server {
	return &Server{repo: r, matcher: matcher, jwt: jwt, hub: hub, history: hist, log: log}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /prediction/windows", s.handleActiveWindows)
	mux.HandleFunc("GET /prediction/windows/{id}", s.handleGetWindow)
	mux.HandleFunc("GET /prediction/history", s.handlePriceHistory)
	mux.HandleFunc("GET /prediction/book", s.handleOrderBook)
	mux.HandleFunc("POST /prediction/orders", s.handlePlaceOrder)
	mux.HandleFunc("POST /prediction/orders/{id}/cancel", s.handleCancelOrder)
	mux.HandleFunc("POST /prediction/positions/sell", s.handleSellPosition)
	mux.HandleFunc("GET /prediction/orders", s.handleUserOrders)
	mux.HandleFunc("GET /prediction/positions", s.handleUserPositions)
	mux.HandleFunc("GET /prediction/ws", s.hub.ServeWS)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// authenticate mirrors Dex-Backend's Server.authenticate(): cookie first,
// then Authorization: Bearer header, verified against the same JWT_SECRET.
func (s *Server) authenticate(r *http.Request) (*auth.Claims, error) {
	var token string
	if c, err := r.Cookie(sessionCookie); err == nil {
		token = c.Value
	} else if h := r.Header.Get("Authorization"); len(h) > 7 && h[:7] == "Bearer " {
		token = h[7:]
	}
	if token == "" {
		s.log.Warn("authenticate: no token in cookie or Authorization header")
		return nil, http.ErrNoCookie
	}
	claims, err := s.jwt.Verify(token)
	if err != nil {
		// Deliberately logs only the failure reason, never the token itself
		// (it's a bearer credential) — this is diagnostic-only so a 401 from
		// the frontend isn't a black box distinguishing "no token sent" from
		// "token sent but rejected" from "signature/secret mismatch".
		s.log.Warn("authenticate: token verify failed", "err", err)
	}
	return claims, err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type windowView struct {
	ID              int64  `json:"id"`
	Market          string `json:"market"`
	Duration        string `json:"duration"`
	Status          string `json:"status"`
	OpeningPrice    string `json:"openingPrice,omitempty"`
	TargetPrice     string `json:"targetPrice,omitempty"`
	ResolutionPrice string `json:"resolutionPrice,omitempty"`
	CommitHash      string `json:"commitHash"`
	StartTime       string `json:"startTime"`
	EndTime         string `json:"endTime"`
}

func toWindowView(win *models.Window) windowView {
	v := windowView{
		ID: win.ID, Market: string(win.Market), Duration: string(win.Duration),
		Status: string(win.Status), CommitHash: win.CommitHash,
		StartTime: win.StartTime.Format("2006-01-02T15:04:05Z07:00"),
		EndTime:   win.EndTime.Format("2006-01-02T15:04:05Z07:00"),
	}
	if win.Status != models.WindowCommitted {
		v.OpeningPrice = win.OpeningPrice.Decimal.String()
		v.TargetPrice = win.TargetPrice.Decimal.String()
	}
	if win.Status == models.WindowSettled {
		v.ResolutionPrice = win.ResolutionPrice.Decimal.String()
	}
	return v
}

func (s *Server) handleActiveWindows(w http.ResponseWriter, r *http.Request) {
	markets := []models.Market{models.MarketBTC, models.MarketETH, models.MarketSOL}
	durations := []models.Duration{models.Duration5m, models.Duration15m}
	var out []windowView
	for _, market := range markets {
		for _, duration := range durations {
			win, err := s.repo.ActiveWindow(r.Context(), market, duration)
			if err != nil {
				continue
			}
			out = append(out, toWindowView(win))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetWindow returns one window by id regardless of status — used by
// the frontend to label historical orders/positions against a settled
// round that's no longer any market's "active" window.
func (s *Server) handleGetWindow(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid window id")
		return
	}
	win, err := s.repo.GetWindow(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "window not found")
		return
	}
	writeJSON(w, http.StatusOK, toWindowView(win))
}

type historyPointView struct {
	TimestampMs  int64  `json:"timestampMs"`
	CurrentPrice string `json:"currentPrice"`
	YesPrice     string `json:"yesPrice"`
}

// handlePriceHistory returns every recorded tick for a window since it
// opened, so a browser opening the page mid-round can render the chart from
// the round's actual start instead of building it up from page-open.
func (s *Server) handlePriceHistory(w http.ResponseWriter, r *http.Request) {
	windowID, err := strconv.ParseInt(r.URL.Query().Get("windowId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid or missing windowId")
		return
	}
	points, err := s.history.Get(r.Context(), windowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load price history")
		return
	}
	out := make([]historyPointView, len(points))
	for i, p := range points {
		out[i] = historyPointView{TimestampMs: p.TimestampMs, CurrentPrice: p.CurrentPrice, YesPrice: p.YesPrice}
	}
	writeJSON(w, http.StatusOK, out)
}

type bookLevelView struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

func (s *Server) handleOrderBook(w http.ResponseWriter, r *http.Request) {
	windowID, err := strconv.ParseInt(r.URL.Query().Get("windowId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid or missing windowId")
		return
	}
	yes, err := s.repo.AggregatedBook(r.Context(), windowID, models.SideYes)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load order book")
		return
	}
	no, err := s.repo.AggregatedBook(r.Context(), windowID, models.SideNo)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load order book")
		return
	}
	toView := func(levels []repo.BookLevel) []bookLevelView {
		out := make([]bookLevelView, len(levels))
		for i, l := range levels {
			out[i] = bookLevelView{Price: l.Price, Size: l.Size}
		}
		return out
	}
	writeJSON(w, http.StatusOK, map[string]any{"yes": toView(yes), "no": toView(no)})
}

type placeOrderReq struct {
	WindowID int64  `json:"windowId"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Size     string `json:"size"`
}

func (s *Server) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req placeOrderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	side := models.OrderSide(req.Side)
	if side != models.SideYes && side != models.SideNo {
		writeError(w, http.StatusBadRequest, "side must be 'yes' or 'no'")
		return
	}
	price, err := decimal.NewFromString(req.Price)
	if err != nil || price.LessThanOrEqual(decimal.Zero) || price.GreaterThanOrEqual(decimal.NewFromInt(1)) {
		writeError(w, http.StatusBadRequest, "price must be between 0 and 1")
		return
	}
	size, err := decimal.NewFromString(req.Size)
	if err != nil || size.LessThanOrEqual(decimal.Zero) {
		writeError(w, http.StatusBadRequest, "invalid size")
		return
	}

	win, err := s.repo.GetWindow(r.Context(), req.WindowID)
	if err != nil {
		writeError(w, http.StatusNotFound, "window not found")
		return
	}
	if win.Status != models.WindowOpen {
		writeError(w, http.StatusConflict, "window is not open for orders")
		return
	}

	order := &models.Order{WindowID: req.WindowID, UserID: claims.UserID, Side: side, Price: price, Size: size}
	id, fills, err := s.matcher.PlaceOrder(r.Context(), order)
	if err != nil {
		s.log.Error("place order", "err", err, "user_id", claims.UserID)
		writeError(w, http.StatusInternalServerError, "failed to place order")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"orderId":    id,
		"filledSize": order.FilledSize.String(),
		"status":     order.Status,
		"fillCount":  len(fills),
	})
}

type sellPositionReq struct {
	WindowID int64  `json:"windowId"`
	Side     string `json:"side"`
	Size     string `json:"size"`
	// MinPrice is the lowest price (in YES-probability terms for the side
	// being sold) the user accepts for the closed portion — protects against
	// selling into a much worse price than the last quoted one. Required.
	MinPrice string `json:"minPrice"`
}

func (s *Server) handleSellPosition(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req sellPositionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	side := models.OrderSide(req.Side)
	if side != models.SideYes && side != models.SideNo {
		writeError(w, http.StatusBadRequest, "side must be 'yes' or 'no'")
		return
	}
	size, err := decimal.NewFromString(req.Size)
	if err != nil || size.LessThanOrEqual(decimal.Zero) {
		writeError(w, http.StatusBadRequest, "invalid size")
		return
	}
	minPrice, err := decimal.NewFromString(req.MinPrice)
	if err != nil || minPrice.LessThanOrEqual(decimal.Zero) || minPrice.GreaterThanOrEqual(decimal.NewFromInt(1)) {
		writeError(w, http.StatusBadRequest, "minPrice must be between 0 and 1")
		return
	}

	win, err := s.repo.GetWindow(r.Context(), req.WindowID)
	if err != nil {
		writeError(w, http.StatusNotFound, "window not found")
		return
	}
	if win.Status != models.WindowOpen {
		writeError(w, http.StatusConflict, "window is not open for trading")
		return
	}

	orderID, filled, err := s.matcher.Sell(r.Context(), req.WindowID, claims.UserID, side, size, minPrice)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"orderId":    orderID,
		"filledSize": filled.String(),
	})
}

func (s *Server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}
	order, err := s.repo.GetOrder(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if order.UserID != claims.UserID {
		writeError(w, http.StatusForbidden, "not your order")
		return
	}
	if err := s.matcher.CancelOrder(r.Context(), id, claims.UserID, order.Side, order.Price); err != nil {
		writeError(w, http.StatusConflict, "unable to cancel order")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (s *Server) handleUserOrders(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	orders, err := s.repo.UserOrders(r.Context(), claims.UserID, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load orders")
		return
	}
	writeJSON(w, http.StatusOK, orders)
}

func (s *Server) handleUserPositions(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	positions, err := s.repo.UserPositions(r.Context(), claims.UserID, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load positions")
		return
	}
	writeJSON(w, http.StatusOK, positions)
}
