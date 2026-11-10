package booking

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Feliper83/transitflow-go/internal/httputil"
	"github.com/Feliper83/transitflow-go/internal/metrics"
	"github.com/go-chi/chi/v5"
)

// CreateBookingRequest is the body of POST /api/bookings.
type CreateBookingRequest struct {
	TripID        int64  `json:"tripId"`
	PassengerName string `json:"passengerName"`
	SeatCount     int    `json:"seatCount"`
}

// Handler wires HTTP routes to the booking Orchestrator.
type Handler struct {
	orchestrator *Orchestrator
}

// NewHandler builds a Handler.
func NewHandler(orchestrator *Orchestrator) *Handler {
	return &Handler{orchestrator: orchestrator}
}

// Routes returns the chi router for booking-service.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(metrics.HTTPMiddleware)
	r.Use(httputil.Correlation)
	r.Get("/health", httputil.Health())
	r.Handle("/metrics", metrics.Handler())
	r.Route("/api/bookings", func(r chi.Router) {
		r.Post("/", h.create)
		r.Get("/{id}", h.get)
	})
	return r
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req CreateBookingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.TripID <= 0 || req.PassengerName == "" || req.SeatCount <= 0 {
		httputil.WriteError(w, http.StatusBadRequest, "tripId, passengerName and positive seatCount are required")
		return
	}
	correlationID := httputil.CorrelationID(r.Context())
	booking, err := h.orchestrator.Book(r.Context(), req.TripID, req.PassengerName, req.SeatCount, correlationID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusAccepted, booking)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid booking id")
		return
	}
	booking, err := h.orchestrator.Get(r.Context(), id)
	if errors.Is(err, ErrBookingNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "booking not found")
		return
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, booking)
}
