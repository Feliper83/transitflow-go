// Package booking implements booking-service, the SAGA orchestrator. It owns the
// booking lifecycle, calls trip-service synchronously to hold a seat, and drives
// payment asynchronously through a transactional outbox.
package booking

import (
	"context"
	"fmt"
)

// Booking lifecycle states: PENDING -> SEAT_RESERVED -> CONFIRMED | CANCELLED.
const (
	StatusPending      = "PENDING"
	StatusSeatReserved = "SEAT_RESERVED"
	StatusConfirmed    = "CONFIRMED"
	StatusCancelled    = "CANCELLED"
)

// Booking is the passenger's intent and its lifecycle state.
type Booking struct {
	ID            int64   `json:"id"`
	TripID        int64   `json:"tripId"`
	PassengerName string  `json:"passengerName"`
	SeatCount     int     `json:"seatCount"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"`
	CorrelationID string  `json:"-"`
	FailureReason string  `json:"failureReason,omitempty"`
}

// IsTerminal reports whether the booking has reached a final state.
func (b *Booking) IsTerminal() bool {
	return b.Status == StatusConfirmed || b.Status == StatusCancelled
}

// Ref is the reference used as the idempotency key for the seat hold in
// trip-service.
func (b *Booking) Ref() string { return fmt.Sprintf("booking-%d", b.ID) }

// TripView is the subset of a trip that the orchestrator needs (for pricing).
type TripView struct {
	ID           int64   `json:"id"`
	RouteName    string  `json:"routeName"`
	PricePerSeat float64 `json:"pricePerSeat"`
}

// TripClient is the synchronous client for trip-service: seat reservation and
// its compensation.
type TripClient interface {
	GetTrip(ctx context.Context, tripID int64) (*TripView, error)
	// Reserve returns true if the seat was held, false if the trip had no capacity.
	Reserve(ctx context.Context, tripID int64, bookingRef string, seatCount int) (bool, error)
	Release(ctx context.Context, bookingRef string) error
}

// Store is the transactional persistence contract for the SAGA. Each mutating
// method commits the booking state change and its outbox event in the same
// database transaction (transactional outbox).
type Store interface {
	Get(ctx context.Context, id int64) (*Booking, error)
	CreatePending(ctx context.Context, tripID int64, passengerName string, seatCount int, amount float64, correlationID string) (*Booking, error)
	ReserveAndRequestPayment(ctx context.Context, bookingID int64) error
	CancelNoSeat(ctx context.Context, bookingID int64) error
	Confirm(ctx context.Context, bookingID int64) error
	CancelAfterPaymentFailure(ctx context.Context, bookingID int64, reason string) error
}
