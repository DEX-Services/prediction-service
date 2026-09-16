package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dex/prediction-service/internal/api"
	"github.com/dex/prediction-service/internal/auth"
	"github.com/dex/prediction-service/internal/backendclient"
	"github.com/dex/prediction-service/internal/db"
	"github.com/dex/prediction-service/internal/history"
	"github.com/dex/prediction-service/internal/index"
	"github.com/dex/prediction-service/internal/repo"
	"github.com/dex/prediction-service/internal/round"
	"github.com/dex/prediction-service/internal/wshub"

	"github.com/joho/godotenv"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if err := godotenv.Load(); err != nil {
		log.Info("no .env file, using env vars")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgURI := mustEnv(log, "POSTGRES_SERVICE_URI")
	redisURI := mustEnv(log, "REDIS_SERVICE_URI")
	jwtSecret := mustEnv(log, "JWT_SECRET")
	port := os.Getenv("PREDICTION_PORT")
	if port == "" {
		port = "8084"
	}

	pool, err := db.Connect(ctx, pgURI)
	if err != nil {
		log.Error("db connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	priceReader, err := index.New(ctx, redisURI, "price", 10*time.Second)
	if err != nil {
		log.Error("price reader connect", "err", err)
		os.Exit(1)
	}
	defer priceReader.Close()

	historyStore, err := history.New(ctx, redisURI)
	if err != nil {
		log.Error("history store connect", "err", err)
		os.Exit(1)
	}
	defer historyStore.Close()

	client, err := backendclient.New()
	if err != nil {
		log.Error("backend client", "err", err)
		os.Exit(1)
	}

	r := repo.New(pool)
	matcher := round.NewMatcher(r, client)
	hub := wshub.NewHub(log)
	jwtIssuer := auth.NewJWTIssuer(jwtSecret, 24*time.Hour)

	manager := round.NewManager(r, priceReader, matcher, client, historyStore, log, func(t round.Tick) {
		hub.BroadcastJSON(map[string]any{
			"type":          "tick",
			"windowId":      t.WindowID,
			"market":        t.Market,
			"duration":      t.Duration,
			"currentPrice":  t.CurrentPrice.String(),
			"targetPrice":   t.TargetPrice.String(),
			"yesPrice":      t.YesPrice.String(),
			"noPrice":       t.NoPrice.String(),
			"timeRemaining": t.TimeRemaining.Milliseconds(),
			"status":        t.Status,
		})
	})

	go func() {
		if err := manager.Start(ctx); err != nil {
			log.Error("round manager stopped", "err", err)
		}
	}()

	server := api.NewServer(r, matcher, jwtIssuer, hub, historyStore, log)
	corsOrigins := os.Getenv("WS_ALLOWED_ORIGINS")
	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: api.CORS(corsOrigins, server.Routes()),
	}

	go func() {
		log.Info("prediction-service listening", "port", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func mustEnv(log *slog.Logger, key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Error("missing required env var", "key", key)
		os.Exit(1)
	}
	return v
}
