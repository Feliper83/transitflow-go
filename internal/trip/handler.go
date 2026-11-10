package trip

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Feliper83/transitflow-go/internal/httputil"
	"github.com/go-chi/chi/v5"
)

// ReserveRequest is the body of a reservation call.
type ReserveRequest struct {
	BookingRef string `json:"bookingRef"`
	SeatCount  int    `json:"seatCount"`
}

// Handler wires HTTP routes to the trip Service.
type Handler struct {
	svc *Service
}

// NewHandler builds a Handler.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Routes returns the chi router for trip-service.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(httputil.Correlation)
	r.Get("/health", httputil.Health())
	r.Route("/api/trips", func(r chi.Router) {
		r.Get("/", h.list)
		r.Get("/{id}", h.get)
		r.Post("/{id}/reservations", h.reserve)
		r.Post("/reservations/{bookingRef}/release", h.release)
	})
	return r
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	trips, err := h.svc.ListTrips(r.Context())
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if trips == nil {
		trips = []Trip{}
	}
	httputil.WriteJSON(w, http.StatusOK, trips)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid trip id")
		return
	}
	t, err := h.svc.GetTrip(r.Context(), id)
	if errors.Is(err, ErrTripNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "trip not found")
		return
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, t)
}

func (h *Handler) reserve(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid trip id")
		return
	}
	var req ReserveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.BookingRef == "" || req.SeatCount <= 0 {
		httputil.WriteError(w, http.StatusBadRequest, "bookingRef and positive seatCount are required")
		return
	}
	result, err := h.svc.Reserve(r.Context(), id, req.BookingRef, req.SeatCount)
	if errors.Is(err, ErrTripNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "trip not found")
		return
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	status := http.StatusOK
	if !result.Reserved {
		status = http.StatusConflict
	}
	httputil.WriteJSON(w, status, result)
}

func (h *Handler) release(w http.ResponseWriter, r *http.Request) {
	bookingRef := chi.URLParam(r, "bookingRef")
	if err := h.svc.Release(r.Context(), bookingRef); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
