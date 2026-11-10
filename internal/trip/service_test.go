package trip

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

// fakeRepo is an in-memory Repository that records whether the persisting
// methods were invoked, so tests can assert that idempotent / rejected paths do
// not touch inventory.
type fakeRepo struct {
	hold *SeatHold
	trip *Trip

	reservationApplied bool
	releaseApplied     bool
}

func (f *fakeRepo) ListTrips(ctx context.Context) ([]Trip, error) { return nil, nil }
func (f *fakeRepo) GetTrip(ctx context.Context, id int64) (*Trip, error) {
	if f.trip == nil {
		return nil, ErrTripNotFound
	}
	return f.trip, nil
}
func (f *fakeRepo) FindHold(ctx context.Context, bookingRef string) (*SeatHold, error) {
	return f.hold, nil
}
func (f *fakeRepo) ApplyReservation(ctx context.Context, t *Trip, hold *SeatHold) error {
	f.reservationApplied = true
	return nil
}
func (f *fakeRepo) ApplyRelease(ctx context.Context, t *Trip, hold *SeatHold) error {
	f.releaseApplied = true
	return nil
}

func testService(repo Repository) *Service {
	return NewService(repo, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestReserveWhenCapacityAvailable(t *testing.T) {
	repo := &fakeRepo{trip: &Trip{ID: 1, Capacity: 40, SeatsAvailable: 40, PricePerSeat: 120.00}}
	result, err := testService(repo).Reserve(context.Background(), 1, "booking-1", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Reserved {
		t.Fatalf("expected reserved=true, got false")
	}
	if !repo.reservationApplied {
		t.Fatalf("expected ApplyReservation to be called")
	}
}

func TestRejectWhenNotEnoughSeats(t *testing.T) {
	repo := &fakeRepo{trip: &Trip{ID: 1, Capacity: 40, SeatsAvailable: 1}}
	result, err := testService(repo).Reserve(context.Background(), 1, "booking-2", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Reserved {
		t.Fatalf("expected reserved=false for insufficient capacity")
	}
	if repo.reservationApplied {
		t.Fatalf("inventory must not be touched when capacity is insufficient")
	}
}

func TestReservationIsIdempotent(t *testing.T) {
	repo := &fakeRepo{hold: &SeatHold{TripID: 1, BookingRef: "booking-3", SeatCount: 2, Status: StatusHeld}}
	result, err := testService(repo).Reserve(context.Background(), 1, "booking-3", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Reserved {
		t.Fatalf("expected existing held reservation to report reserved=true")
	}
	if repo.reservationApplied {
		t.Fatalf("duplicate reservation must not decrement inventory again")
	}
}

func TestReleaseIsIdempotentForUnknownHold(t *testing.T) {
	repo := &fakeRepo{} // no hold
	if err := testService(repo).Release(context.Background(), "booking-x"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.releaseApplied {
		t.Fatalf("releasing an unknown hold must be a no-op")
	}
}

func TestReleaseReturnsSeats(t *testing.T) {
	repo := &fakeRepo{
		hold: &SeatHold{TripID: 1, BookingRef: "booking-4", SeatCount: 2, Status: StatusHeld},
		trip: &Trip{ID: 1, Capacity: 40, SeatsAvailable: 38},
	}
	if err := testService(repo).Release(context.Background(), "booking-4"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !repo.releaseApplied {
		t.Fatalf("expected ApplyRelease to be called for a held reservation")
	}
	if repo.trip.SeatsAvailable != 40 {
		t.Fatalf("expected seats restored to 40, got %d", repo.trip.SeatsAvailable)
	}
}
