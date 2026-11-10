// Command trip is the entrypoint for trip-service: an HTTP service that owns
// seat inventory and serves idempotent reserve / release for the booking SAGA.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Feliper83/transitflow-go/internal/config"
	"github.com/Feliper83/transitflow-go/internal/pg"
	"github.com/Feliper83/transitflow-go/internal/trip"
)

func main() {
	log := config.Logger("trip-service")

	dsn := config.String("DATABASE_URL", "postgres://transitflow:transitflow@localhost:5433/trips?sslmode=disable")
	addr := ":" + config.String("PORT", "8081")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := trip.NewPgStore(pool)
	if err := store.Migrate(ctx); err != nil {
		log.Error("migration failed", "error", err)
		os.Exit(1)
	}

	handler := trip.NewHandler(trip.NewService(store, log))
	srv := &http.Server{Addr: addr, Handler: handler.Routes()}

	go func() {
		log.Info("trip-service listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
