// Command payment is the entrypoint for payment-service: a Kafka consumer that
// processes payment commands idempotently and emits payment results. It also
// exposes a small HTTP health endpoint.
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
	"github.com/Feliper83/transitflow-go/internal/events"
	"github.com/Feliper83/transitflow-go/internal/httputil"
	"github.com/Feliper83/transitflow-go/internal/kafkautil"
	"github.com/Feliper83/transitflow-go/internal/metrics"
	"github.com/Feliper83/transitflow-go/internal/payment"
	"github.com/Feliper83/transitflow-go/internal/pg"
)

func main() {
	log := config.Logger("payment-service")

	dsn := config.String("DATABASE_URL", "postgres://transitflow:transitflow@localhost:5435/payments?sslmode=disable")
	brokers := config.String("KAFKA_BROKERS", "localhost:9092")
	declineAbove := config.Float("TRANSITFLOW_PAYMENT_DECLINE_ABOVE", 500.00)
	addr := ":" + config.String("PORT", "8083")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := payment.NewPgStore(pool)
	if err := store.Migrate(ctx); err != nil {
		log.Error("migration failed", "error", err)
		os.Exit(1)
	}

	svc := payment.NewService(store, declineAbove, log)

	reader := kafkautil.NewReader(brokers, events.TopicPaymentCommands, "payment-service")
	defer reader.Close()
	writer := kafkautil.NewWriter(brokers)
	defer writer.Close()

	consumer := payment.NewConsumer(reader, writer, svc, log)
	go func() {
		if err := consumer.Run(ctx); err != nil {
			log.Error("consumer stopped with error", "error", err)
			stop()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httputil.Health())
	mux.Handle("/metrics", metrics.Handler())
	srv := &http.Server{Addr: addr, Handler: metrics.HTTPMiddleware(mux)}
	go func() {
		log.Info("payment-service health listening", "addr", addr, "declineAbove", declineAbove)
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
