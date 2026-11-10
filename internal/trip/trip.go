// Package trip implements trip-service: seat inventory with idempotent seat
// reservation and a compensating release used by the booking SAGA.
package trip

import "time"

// Seat hold lifecycle states.
const (
	StatusHeld     = "HELD"
	StatusReleased = "RELEASED"
)

// Trip is a scheduled service with a seat inventory.
type Trip struct {
	ID             int64     `json:"id"`
	RouteName      string    `json:"routeName"`
	Origin         string    `json:"origin"`
	Destination    string    `json:"destination"`
	DepartureTime  time.Time `json:"departureTime"`
	Capacity       int       `json:"-"`
	SeatsAvailable int       `json:"seatsAvailable"`
	PricePerSeat   float64   `json:"pricePerSeat"`
	Version        int64     `json:"-"`
}

// HasCapacityFor reports whether at least seats are available.
func (t *Trip) HasCapacityFor(seats int) bool { return t.SeatsAvailable >= seats }

// SeatHold records seats held for a booking. BookingRef is unique, which makes
// reservation idempotent: a repeated request for the same reference returns the
// existing hold instead of decrementing inventory again.
type SeatHold struct {
	ID         int64
	TripID     int64
	BookingRef string
	SeatCount  int
	Status     string
}

// IsHeld reports whether the hold still holds seats (i.e. is not released).
func (h *SeatHold) IsHeld() bool { return h.Status == StatusHeld }

// ReservationResult is the response of a reserve call.
type ReservationResult struct {
	Reserved   bool   `json:"reserved"`
	BookingRef string `json:"bookingRef"`
	SeatCount  int    `json:"seatCount"`
}
