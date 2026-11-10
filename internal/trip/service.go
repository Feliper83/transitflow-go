package trip

import (
	"context"
	"errors"
	"log/slog"
)

// ErrTripNotFound is returned when a trip id does not exist.
var ErrTripNotFound = errors.New("trip not found")

// Repository is the persistence contract for trip-service. The pgx
// implementation runs ApplyReservation / ApplyRelease as a single database
// transaction; tests substitute an in-memory fake.
type Repository interface {
	ListTrips(ctx context.Context) ([]Trip, error)
	GetTrip(ctx context.Context, id int64) (*Trip, error)
	FindHold(ctx context.Context, bookingRef string) (*SeatHold, error)
	// ApplyReservation persists the seat decrement and the new hold atomically.
	ApplyReservation(ctx context.Context, trip *Trip, hold *SeatHold) error
	// ApplyRelease persists the seat increment and the hold's released status.
	ApplyRelease(ctx context.Context, trip *Trip, hold *SeatHold) error
}

// Service holds the seat-inventory business logic.
type Service struct {
	repo Repository
	log  *slog.Logger
}

// NewService builds a trip Service.
func NewService(repo Repository, log *slog.Logger) *Service {
	return &Service{repo: repo, log: log}
}

// ListTrips returns all trips.
func (s *Service) ListTrips(ctx context.Context) ([]Trip, error) { return s.repo.ListTrips(ctx) }

// GetTrip returns a trip or ErrTripNotFound.
func (s *Service) GetTrip(ctx context.Context, id int64) (*Trip, error) {
	return s.repo.GetTrip(ctx, id)
}

// Reserve holds seats for a booking. It is idempotent on bookingRef: a repeated
// request for a reference that already holds seats returns the existing hold
// without decrementing inventory again. Returns Reserved=false (no error) when
// the trip lacks capacity.
func (s *Service) Reserve(ctx context.Context, tripID int64, bookingRef string, seatCount int) (ReservationResult, error) {
	existing, err := s.repo.FindHold(ctx, bookingRef)
	if err != nil {
		return ReservationResult{}, err
	}
	if existing != nil {
		s.log.Info("reservation already exists, returning idempotently",
			"bookingRef", bookingRef, "status", existing.Status)
		return ReservationResult{Reserved: existing.IsHeld(), BookingRef: existing.BookingRef, SeatCount: existing.SeatCount}, nil
	}

	t, err := s.repo.GetTrip(ctx, tripID)
	if err != nil {
		return ReservationResult{}, err
	}
	if !t.HasCapacityFor(seatCount) {
		s.log.Info("reservation rejected: insufficient capacity",
			"bookingRef", bookingRef, "tripId", tripID, "available", t.SeatsAvailable, "requested", seatCount)
		return ReservationResult{Reserved: false, BookingRef: bookingRef, SeatCount: seatCount}, nil
	}

	t.SeatsAvailable -= seatCount
	hold := &SeatHold{TripID: tripID, BookingRef: bookingRef, SeatCount: seatCount, Status: StatusHeld}
	if err := s.repo.ApplyReservation(ctx, t, hold); err != nil {
		return ReservationResult{}, err
	}
	s.log.Info("seats reserved", "seatCount", seatCount, "tripId", tripID, "bookingRef", bookingRef)
	return ReservationResult{Reserved: true, BookingRef: bookingRef, SeatCount: seatCount}, nil
}

// Release is the SAGA compensation for a failed booking: it returns previously
// held seats. Idempotent — releasing an unknown or already-released hold is a
// no-op.
func (s *Service) Release(ctx context.Context, bookingRef string) error {
	hold, err := s.repo.FindHold(ctx, bookingRef)
	if err != nil {
		return err
	}
	if hold == nil {
		s.log.Info("release ignored: no hold found", "bookingRef", bookingRef)
		return nil
	}
	if !hold.IsHeld() {
		s.log.Info("release ignored: already released", "bookingRef", bookingRef)
		return nil
	}
	t, err := s.repo.GetTrip(ctx, hold.TripID)
	if err != nil {
		return err
	}
	t.SeatsAvailable += hold.SeatCount
	if t.SeatsAvailable > t.Capacity {
		t.SeatsAvailable = t.Capacity
	}
	hold.Status = StatusReleased
	if err := s.repo.ApplyRelease(ctx, t, hold); err != nil {
		return err
	}
	s.log.Info("seats released", "seatCount", hold.SeatCount, "tripId", t.ID, "bookingRef", bookingRef)
	return nil
}
