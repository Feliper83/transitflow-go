// Package httputil holds small HTTP helpers shared by the services: correlation
// id propagation, JSON responses, and a health endpoint.
package httputil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
)

// CorrelationHeader is the HTTP header that carries the correlation id across
// services. The same value is copied into every event the flow emits.
const CorrelationHeader = "X-Correlation-Id"

type ctxKey int

const correlationKey ctxKey = 0

// NewCorrelationID returns a random hex correlation id.
func NewCorrelationID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "corr-unknown"
	}
	return hex.EncodeToString(b)
}

// CorrelationID extracts the correlation id stored in the context, if any.
func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey).(string); ok {
		return v
	}
	return ""
}

// Correlation is middleware that reads X-Correlation-Id (minting one if absent),
// stores it on the request context, and echoes it on the response.
func Correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationHeader)
		if id == "" {
			id = NewCorrelationID()
		}
		w.Header().Set(CorrelationHeader, id)
		ctx := context.WithValue(r.Context(), correlationKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// WriteJSON writes v as a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		if err := json.NewEncoder(w).Encode(v); err != nil {
			slog.Error("failed to encode response", "error", err)
		}
	}
}

// WriteError writes a JSON {"error": message} body with the given status.
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

// Health returns a handler that reports liveness as JSON.
func Health() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	}
}
