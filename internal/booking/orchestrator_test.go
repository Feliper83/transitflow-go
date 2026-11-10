package booking

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

// fakeStore is a spy Store that records compensation-relevant calls.
type fakeStore struct {
	bookings map[int64]*Booking

	cancelAfterFailureCalls int
	confirmCalls            int
}

func (f *fakeStore) Get(ctx context.Context, id int64) (*Booking, error) {
	return f.bookings[id], nil
}
func (f *fakeStore) CreatePending(ctx context.Context, tripID int64, passengerName string, seatCount int, amount float64, correlationID string) (*Booking, error) {
	b := &Booking{ID: int64(len(f.bookings) + 1), TripID: tripID, PassengerName: passengerName,
		SeatCount: seatCount, Amount: amount, Status: StatusPending, CorrelationID: correlationID}
	f.bookings[b.ID] = b
	return b, nil
}
func (f *fakeStore) ReserveAndRequestPayment(ctx context.Context, bookingID int64) error {
	f.bookings[bookingID].Status = StatusSeatReserved
	return nil
}
func (f *fakeStore) CancelNoSeat(ctx context.Context, bookingID int64) error {
	f.bookings[bookingID].Status = StatusCancelled
	return nil
}
func (f *fakeStore) Confirm(ctx context.Context, bookingID int64) error {
	f.confirmCalls++
	f.bookings[bookingID].Status = StatusConfirmed
	return nil
}
func (f *fakeStore) CancelAfterPaymentFailure(ctx context.Context, bookingID int64, reason string) error {
	f.cancelAfterFailureCalls++
	b := f.bookings[bookingID]
	b.Status = StatusCancelled
	b.FailureReason = reason
	return nil
}

// fakeTripClient is a spy TripClient recording release calls.
type fakeTripClient struct {
	releaseCalls []string
}

func (f *fakeTripClient) GetTrip(ctx context.Context, tripID int64) (*TripView, error) {
	return &TripView{ID: tripID, PricePerSeat: 120.00}, nil
}
func (f *fakeTripClient) Reserve(ctx context.Context, tripID int64, bookingRef string, seatCount int) (bool, error) {
	return true, nil
}
func (f *fakeTripClient) Release(ctx context.Context, bookingRef string) error {
	f.releaseCalls = append(f.releaseCalls, bookingRef)
	return nil
}

func testOrchestrator(store Store, tc TripClient) *Orchestrator {
	return NewOrchestrator(store, tc, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestCompensationReleasesSeatThenCancels(t *testing.T) {
	store := &fakeStore{bookings: map[int64]*Booking{
		10: {ID: 10, PassengerName: "Ada", Status: StatusSeatReserved},
	}}
	tc := &fakeTripClient{}

	if err := testOrchestrator(store, tc).Compensate(context.Background(), 10, "payment declined"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tc.releaseCalls) != 1 || tc.releaseCalls[0] != "booking-10" {
		t.Fatalf("expected release of booking-10, got %v", tc.releaseCalls)
	}
	if store.cancelAfterFailureCalls != 1 {
		t.Fatalf("expected one cancelAfterPaymentFailure, got %d", store.cancelAfterFailureCalls)
	}
}

func TestCompensationIsSkippedForTerminalBooking(t *testing.T) {
	store := &fakeStore{bookings: map[int64]*Booking{
		11: {ID: 11, PassengerName: "Ada", Status: StatusConfirmed},
	}}
	tc := &fakeTripClient{}

	if err := testOrchestrator(store, tc).Compensate(context.Background(), 11, "late payment failure"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Already terminal: no seat release, no state change.
	if len(tc.releaseCalls) != 0 {
		t.Fatalf("expected no release for terminal booking, got %v", tc.releaseCalls)
	}
	if store.cancelAfterFailureCalls != 0 {
		t.Fatalf("expected no cancellation for terminal booking, got %d", store.cancelAfterFailureCalls)
	}
}

func TestBookMovesToSeatReservedAndRequestsPayment(t *testing.T) {
	store := &fakeStore{bookings: map[int64]*Booking{}}
	tc := &fakeTripClient{}

	b, err := testOrchestrator(store, tc).Book(context.Background(), 1, "Ada", 1, "corr")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Status != StatusSeatReserved {
		t.Fatalf("expected SEAT_RESERVED, got %s", b.Status)
	}
	if b.Amount != 120.00 {
		t.Fatalf("expected amount 120.00 (price*seats), got %.2f", b.Amount)
	}
}
