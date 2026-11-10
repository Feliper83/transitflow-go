// Command booking is the entrypoint for booking-service, the SAGA orchestrator.
// It serves the booking HTTP API, runs the transactional-outbox relay, and
// consumes payment results to drive the saga to CONFIRMED or compensation.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Feliper83/transitflow-go/internal/booking"
	"github.com/Feliper83/transitflow-go/internal/config"
	"github.com/Feliper83/transitflow-go/internal/events"
	"github.com/Feliper83/transitflow-go/internal/kafkautil"
	"github.com/Feliper83/transitflow-go/internal/pg"
)

func main() {
	log := config.Logger("booking-service")

	dsn := config.String("DATABASE_URL", "postgres://transitflow:transitflow@localhost:5434/bookings?sslmode=disable")
	brokers := config.String("KAFKA_BROKERS", "localhost:9092")
	tripURL := config.String("TRIP_SERVICE_URL", "http://localhost:8081")
	pollInterval := time.Duration(config.Float("OUTBOX_POLL_INTERVAL_MS", 2000)) * time.Millisecond
	addr := ":" + config.String("PORT", "8082")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := booking.NewPgStore(pool, log)
	if err := store.Migrate(ctx); err != nil {
		log.Error("migration failed", "error", err)
		os.Exit(1)
	}

	writer := kafkautil.NewWriter(brokers)
	defer writer.Close()
	tripClient := booking.NewHTTPTripClient(tripURL, log)
	orchestrator := booking.NewOrchestrator(store, tripClient, log)

	// Transactional-outbox relay: drains unsent events to Kafka.
	relay := booking.NewOutboxRelay(pool, writer, pollInterval, log)
	go relay.Run(ctx)

	// Consume payment.events to advance the saga.
	reader := kafkautil.NewReader(brokers, events.TopicPaymentEvents, "booking-service")
	defer reader.Close()
	consumer := booking.NewPaymentConsumer(reader, orchestrator, log)
	go func() {
		if err := consumer.Run(ctx); err != nil {
			log.Error("consumer stopped with error", "error", err)
			stop()
		}
	}()

	handler := booking.NewHandler(orchestrator)
	srv := &http.Server{Addr: addr, Handler: handler.Routes()}
	go func() {
		log.Info("booking-service listening", "addr", addr)
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
