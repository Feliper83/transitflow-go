// Package metrics defines the Prometheus metrics shared across TransitFlow
// services. The metric names are part of the cross-language contract: the Java,
// Go, and Python ports expose the exact same names so one Grafana dashboard
// works against any of them.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Business metrics. Names are fixed and shared across the polyglot ports.
var (
	// BookingsCreated counts bookings created by booking-service.
	BookingsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "transitflow_bookings_created_total",
		Help: "Total number of bookings created.",
	})

	// BookingsTotal counts bookings that reached a terminal state, labelled by
	// status (CONFIRMED / CANCELLED).
	BookingsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "transitflow_bookings_total",
		Help: "Total bookings that reached a terminal state, by status.",
	}, []string{"status"})

	// PaymentsTotal counts processed payments, labelled by result
	// (COMPLETED / FAILED).
	PaymentsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "transitflow_payments_total",
		Help: "Total payments processed, by result.",
	}, []string{"result"})

	// OutboxPending is the current number of unsent transactional-outbox rows.
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "transitflow_outbox_pending",
		Help: "Current number of unsent outbox rows.",
	})

	// SagaDuration observes the wall-clock time from booking creation to the
	// booking reaching a terminal state.
	SagaDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "transitflow_saga_duration_seconds",
		Help:    "Duration from booking creation to terminal state, in seconds.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
	})

	// httpDuration observes HTTP request latency labelled by method, route and
	// status, populated by the HTTP middleware.
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "transitflow_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
)

// ObserveBookingCreated records a booking creation.
func ObserveBookingCreated() { BookingsCreated.Inc() }

// ObserveBookingTerminal records a booking reaching the given terminal status
// and the total saga duration since the booking was created.
func ObserveBookingTerminal(status string, createdAt time.Time) {
	BookingsTotal.WithLabelValues(status).Inc()
	if !createdAt.IsZero() {
		SagaDuration.Observe(time.Since(createdAt).Seconds())
	}
}

// ObservePayment records a processed payment result (COMPLETED / FAILED).
func ObservePayment(result string) { PaymentsTotal.WithLabelValues(result).Inc() }

// SetOutboxPending sets the current unsent-outbox gauge value.
func SetOutboxPending(n int) { OutboxPending.Set(float64(n)) }

// Handler returns the Prometheus /metrics HTTP handler.
func Handler() http.Handler { return promhttp.Handler() }

// statusRecorder captures the response status code for metric labelling.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// routePattern returns a low-cardinality route label: the chi route pattern
// when available (e.g. "/api/bookings/{id}"), falling back to the request path.
func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if p := rctx.RoutePattern(); p != "" {
			return p
		}
	}
	if r.URL.Path != "" {
		return r.URL.Path
	}
	return "unknown"
}

// HTTPMiddleware is a chi-compatible middleware that records request duration
// into transitflow_http_request_duration_seconds, labelled by method, route and
// status. It also works when wrapping a plain http.Handler (falling back to the
// request path for the route label).
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		httpDuration.WithLabelValues(
			r.Method,
			routePattern(r),
			strconv.Itoa(rec.status),
		).Observe(time.Since(start).Seconds())
	})
}
