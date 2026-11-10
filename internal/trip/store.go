package trip

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore is the pgx-backed Repository for trip-service.
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPgStore builds a PgStore.
func NewPgStore(pool *pgxpool.Pool) *PgStore { return &PgStore{pool: pool} }

const schemaSQL = `
CREATE TABLE IF NOT EXISTS trips (
    id              BIGINT       PRIMARY KEY,
    route_name      TEXT         NOT NULL,
    origin          TEXT         NOT NULL,
    destination     TEXT         NOT NULL,
    departure_time  TIMESTAMPTZ  NOT NULL,
    capacity        INT          NOT NULL,
    seats_available INT          NOT NULL,
    price_per_seat  NUMERIC(12,2) NOT NULL,
    version         BIGINT       NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS seat_holds (
    id          BIGSERIAL   PRIMARY KEY,
    trip_id     BIGINT      NOT NULL,
    booking_ref TEXT        NOT NULL UNIQUE,
    seat_count  INT         NOT NULL,
    status      TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);`

const seedSQL = `
INSERT INTO trips (id, route_name, origin, destination, departure_time, capacity, seats_available, price_per_seat, version) VALUES
  (1, 'Bogota Express',  'Bogota',    'Medellin',     '2026-12-01T08:00:00Z', 40, 40, 120.00, 0),
  (2, 'Caribbean Coast', 'Cartagena', 'Barranquilla', '2026-12-01T09:30:00Z', 30, 30,  45.00, 0),
  (3, 'Andes Premium',   'Bogota',    'Cali',         '2026-12-01T07:15:00Z', 20, 20, 600.00, 0)
ON CONFLICT (id) DO NOTHING;`

// Migrate creates the schema and seeds the three demo trips.
func (s *PgStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, seedSQL)
	return err
}

func (s *PgStore) ListTrips(ctx context.Context) ([]Trip, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, route_name, origin, destination, departure_time,
		capacity, seats_available, price_per_seat, version FROM trips ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var trips []Trip
	for rows.Next() {
		var t Trip
		if err := rows.Scan(&t.ID, &t.RouteName, &t.Origin, &t.Destination, &t.DepartureTime,
			&t.Capacity, &t.SeatsAvailable, &t.PricePerSeat, &t.Version); err != nil {
			return nil, err
		}
		trips = append(trips, t)
	}
	return trips, rows.Err()
}

func (s *PgStore) GetTrip(ctx context.Context, id int64) (*Trip, error) {
	return getTrip(ctx, s.pool, id)
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func getTrip(ctx context.Context, q querier, id int64) (*Trip, error) {
	var t Trip
	err := q.QueryRow(ctx, `SELECT id, route_name, origin, destination, departure_time,
		capacity, seats_available, price_per_seat, version FROM trips WHERE id = $1`, id).
		Scan(&t.ID, &t.RouteName, &t.Origin, &t.Destination, &t.DepartureTime,
			&t.Capacity, &t.SeatsAvailable, &t.PricePerSeat, &t.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTripNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *PgStore) FindHold(ctx context.Context, bookingRef string) (*SeatHold, error) {
	var h SeatHold
	err := s.pool.QueryRow(ctx, `SELECT id, trip_id, booking_ref, seat_count, status
		FROM seat_holds WHERE booking_ref = $1`, bookingRef).
		Scan(&h.ID, &h.TripID, &h.BookingRef, &h.SeatCount, &h.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &h, nil
}

// ApplyReservation decrements inventory and inserts the hold in one transaction.
// The UPDATE is guarded by the optimistic version and a capacity check, so
// concurrent reservations cannot oversell.
func (s *PgStore) ApplyReservation(ctx context.Context, t *Trip, hold *SeatHold) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE trips SET seats_available = seats_available - $1, version = version + 1
			WHERE id = $2 AND version = $3 AND seats_available >= $1`, hold.SeatCount, t.ID, t.Version)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errors.New("reservation conflict: trip changed concurrently")
		}
		_, err = tx.Exec(ctx, `INSERT INTO seat_holds (trip_id, booking_ref, seat_count, status)
			VALUES ($1, $2, $3, $4)`, hold.TripID, hold.BookingRef, hold.SeatCount, hold.Status)
		return err
	})
}

// ApplyRelease increments inventory and marks the hold released in one
// transaction.
func (s *PgStore) ApplyRelease(ctx context.Context, t *Trip, hold *SeatHold) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE trips SET seats_available = $1, version = version + 1
			WHERE id = $2`, t.SeatsAvailable, t.ID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE seat_holds SET status = $1 WHERE booking_ref = $2`,
			hold.Status, hold.BookingRef)
		return err
	})
}
