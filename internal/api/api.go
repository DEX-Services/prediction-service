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
	log     *slog.Logger
}

func NewServer(r *repo.Repo, matcher *round.Matcher, jwt *auth.JWTIssuer, hub *wshub.Hub, log *slog.Logger) *Server {
	return &Server{repo: r, matcher: matcher, jwt: jwt, hub: hub, log: log}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /prediction/windows", s.handleActiveWindows)
	mux.HandleFunc("POST /prediction/orders", s.handlePlaceOrder)
	mux.HandleFunc("POST /prediction/orders/{id}/cancel", s.handleCancelOrder)
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
		return nil, http.ErrNoCookie
	}
	return s.jwt.Verify(token)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleActiveWindows(w http.ResponseWriter, r *http.Request) {
	type windowView struct {
		ID           int64  `json:"id"`
		Market       string `json:"market"`
		Duration     string `json:"duration"`
		Status       string `json:"status"`
		OpeningPrice string `json:"openingPrice,omitempty"`
		TargetPrice  string `json:"targetPrice,omitempty"`
		CommitHash   string `json:"commitHash"`
		StartTime    string `json:"startTime"`
		EndTime      string `json:"endTime"`
	}
	markets := []models.Market{models.MarketBTC, models.MarketETH, models.MarketSOL}
	durations := []models.Duration{models.Duration5m, models.Duration15m}
	var out []windowView
	for _, market := range markets {
		for _, duration := range durations {
			win, err := s.repo.ActiveWindow(r.Context(), market, duration)
			if err != nil {
				continue
			}
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
			out = append(out, v)
		}
	}
	writeJSON(w, http.StatusOK, out)
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
