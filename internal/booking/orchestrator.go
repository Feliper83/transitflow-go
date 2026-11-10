package booking

import (
	"context"
	"log/slog"
)

// Orchestrator drives the booking SAGA. Remote calls to trip-service happen
// here, outside any database transaction; the atomic state-change + outbox
// writes are delegated to the Store.
type Orchestrator struct {
	store      Store
	tripClient TripClient
	log        *slog.Logger
}

// NewOrchestrator builds an Orchestrator.
func NewOrchestrator(store Store, tripClient TripClient, log *slog.Logger) *Orchestrator {
	return &Orchestrator{store: store, tripClient: tripClient, log: log}
}

// Get returns a booking by id.
func (o *Orchestrator) Get(ctx context.Context, id int64) (*Booking, error) {
	return o.store.Get(ctx, id)
}

// Book creates a booking and runs it through the SAGA: price it, persist it as
// PENDING, hold a seat synchronously, then either cancel (no capacity) or move
// to SEAT_RESERVED and request payment via the outbox.
func (o *Orchestrator) Book(ctx context.Context, tripID int64, passengerName string, seatCount int, correlationID string) (*Booking, error) {
	trip, err := o.tripClient.GetTrip(ctx, tripID)
	if err != nil {
		return nil, err
	}
	amount := trip.PricePerSeat * float64(seatCount)

	booking, err := o.store.CreatePending(ctx, tripID, passengerName, seatCount, amount, correlationID)
	if err != nil {
		return nil, err
	}
	bookingID := booking.ID

	// Step 1 (synchronous): hold the seat.
	reserved, err := o.tripClient.Reserve(ctx, tripID, booking.Ref(), seatCount)
	if err != nil {
		return nil, err
	}
	if !reserved {
		if err := o.store.CancelNoSeat(ctx, bookingID); err != nil {
			return nil, err
		}
	} else {
		// Step 2: request payment asynchronously via the outbox.
		if err := o.store.ReserveAndRequestPayment(ctx, bookingID); err != nil {
			return nil, err
		}
	}
	return o.store.Get(ctx, bookingID)
}

// Confirm advances a booking to CONFIRMED on payment success (idempotent in the
// Store — a terminal booking is skipped).
func (o *Orchestrator) Confirm(ctx context.Context, bookingID int64) error {
	return o.store.Confirm(ctx, bookingID)
}

// Compensate handles payment failure: release the held seat (remote), then
// record the cancellation. Idempotent: a booking already in a terminal state is
// skipped, so a redelivered PaymentFailed does not double-compensate.
func (o *Orchestrator) Compensate(ctx context.Context, bookingID int64, reason string) error {
	booking, err := o.store.Get(ctx, bookingID)
	if err != nil {
		return err
	}
	if booking.IsTerminal() {
		o.log.Info("booking already terminal, skipping compensation",
			"bookingId", bookingID, "status", booking.Status)
		return nil
	}
	if err := o.tripClient.Release(ctx, booking.Ref()); err != nil {
		return err
	}
	return o.store.CancelAfterPaymentFailure(ctx, bookingID, reason)
}
