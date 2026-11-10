package booking

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/Feliper83/transitflow-go/internal/events"
	"github.com/Feliper83/transitflow-go/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrBookingNotFound is returned when a booking id does not exist.
var ErrBookingNotFound = errors.New("booking not found")

// PgStore is the pgx-backed Store. Every state change and the event it produces
// are written in the same transaction (transactional outbox).
type PgStore struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewPgStore builds a PgStore.
func NewPgStore(pool *pgxpool.Pool, log *slog.Logger) *PgStore {
	return &PgStore{pool: pool, log: log}
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS bookings (
    id             BIGSERIAL    PRIMARY KEY,
    trip_id        BIGINT       NOT NULL,
    passenger_name TEXT         NOT NULL,
    seat_count     INT          NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    status         TEXT         NOT NULL,
    correlation_id TEXT         NOT NULL,
    failure_reason TEXT,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox_events (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    topic       TEXT        NOT NULL,
    message_key TEXT        NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     TEXT        NOT NULL,
    sent        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_unsent ON outbox_events (sent, created_at);`

// Migrate creates the bookings and outbox schema.
func (s *PgStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

const bookingCols = `id, trip_id, passenger_name, seat_count, amount, status, correlation_id, COALESCE(failure_reason, ''), created_at`

func scanBooking(row pgx.Row) (*Booking, error) {
	var b Booking
	err := row.Scan(&b.ID, &b.TripID, &b.PassengerName, &b.SeatCount, &b.Amount,
		&b.Status, &b.CorrelationID, &b.FailureReason, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrBookingNotFound
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (s *PgStore) Get(ctx context.Context, id int64) (*Booking, error) {
	return scanBooking(s.pool.QueryRow(ctx, `SELECT `+bookingCols+` FROM bookings WHERE id = $1`, id))
}

func getTx(ctx context.Context, tx pgx.Tx, id int64) (*Booking, error) {
	return scanBooking(tx.QueryRow(ctx, `SELECT `+bookingCols+` FROM bookings WHERE id = $1`, id))
}

func (s *PgStore) CreatePending(ctx context.Context, tripID int64, passengerName string, seatCount int, amount float64, correlationID string) (*Booking, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `INSERT INTO bookings (trip_id, passenger_name, seat_count, amount, status, correlation_id)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		tripID, passengerName, seatCount, amount, StatusPending, correlationID).Scan(&id)
	if err != nil {
		return nil, err
	}
	metrics.ObserveBookingCreated()
	s.log.Info("created booking (PENDING)", "bookingId", id, "tripId", tripID)
	return s.Get(ctx, id)
}

// writeOutbox inserts a serialized event into the outbox within the given tx.
func writeOutbox(ctx context.Context, tx pgx.Tx, topic, msgKey, eventType string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events (topic, message_key, event_type, payload)
		VALUES ($1, $2, $3, $4)`, topic, msgKey, eventType, string(payload))
	return err
}

// key returns the Kafka message key for a booking: its id as a string.
func key(id int64) string { return strconv.FormatInt(id, 10) }

// ReserveAndRequestPayment marks the booking SEAT_RESERVED and enqueues a
// ProcessPaymentCommand in the outbox, atomically.
func (s *PgStore) ReserveAndRequestPayment(ctx context.Context, bookingID int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		b, err := getTx(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE bookings SET status = $1, updated_at = now() WHERE id = $2`,
			StatusSeatReserved, bookingID); err != nil {
			return err
		}
		cmd := events.ProcessPaymentCommand{BookingID: bookingID, Amount: b.Amount, CorrelationID: b.CorrelationID}
		if err := writeOutbox(ctx, tx, events.TopicPaymentCommands, key(bookingID), "ProcessPaymentCommand", cmd); err != nil {
			return err
		}
		s.log.Info("booking seat reserved; payment requested", "bookingId", bookingID)
		return nil
	})
}

// CancelNoSeat cancels a booking that could not get a seat and emits a cancelled
// BookingEvent, atomically.
func (s *PgStore) CancelNoSeat(ctx context.Context, bookingID int64) error {
	const reason = "No seats available"
	var createdAt time.Time
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		b, err := getTx(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		createdAt = b.CreatedAt
		if _, err := tx.Exec(ctx, `UPDATE bookings SET status = $1, failure_reason = $2, updated_at = now() WHERE id = $3`,
			StatusCancelled, reason, bookingID); err != nil {
			return err
		}
		evt := events.BookingEvent{BookingID: bookingID, Type: events.BookingCancelled,
			PassengerName: b.PassengerName, Reason: reason, CorrelationID: b.CorrelationID}
		if err := writeOutbox(ctx, tx, events.TopicBookingEvents, key(bookingID), "BookingEvent", evt); err != nil {
			return err
		}
		s.log.Info("booking cancelled: no seats available", "bookingId", bookingID)
		return nil
	})
	if err == nil {
		metrics.ObserveBookingTerminal(StatusCancelled, createdAt)
	}
	return err
}

// Confirm moves a booking to CONFIRMED and emits a confirmed BookingEvent,
// atomically. Idempotent: a booking already terminal is skipped.
func (s *PgStore) Confirm(ctx context.Context, bookingID int64) error {
	var (
		confirmed bool
		createdAt time.Time
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		b, err := getTx(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if b.IsTerminal() {
			s.log.Info("booking already terminal, ignoring duplicate payment result",
				"bookingId", bookingID, "status", b.Status)
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE bookings SET status = $1, updated_at = now() WHERE id = $2`,
			StatusConfirmed, bookingID); err != nil {
			return err
		}
		evt := events.BookingEvent{BookingID: bookingID, Type: events.BookingConfirmed,
			PassengerName: b.PassengerName, CorrelationID: b.CorrelationID}
		if err := writeOutbox(ctx, tx, events.TopicBookingEvents, key(bookingID), "BookingEvent", evt); err != nil {
			return err
		}
		confirmed = true
		createdAt = b.CreatedAt
		s.log.Info("booking CONFIRMED", "bookingId", bookingID)
		return nil
	})
	// Count only a real transition, so a redelivered payment result (skipped
	// above) does not double-count the saga.
	if err == nil && confirmed {
		metrics.ObserveBookingTerminal(StatusConfirmed, createdAt)
	}
	return err
}

// CancelAfterPaymentFailure records the cancellation and emits a cancelled
// BookingEvent, atomically.
func (s *PgStore) CancelAfterPaymentFailure(ctx context.Context, bookingID int64, reason string) error {
	var createdAt time.Time
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		b, err := getTx(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		createdAt = b.CreatedAt
		if _, err := tx.Exec(ctx, `UPDATE bookings SET status = $1, failure_reason = $2, updated_at = now() WHERE id = $3`,
			StatusCancelled, reason, bookingID); err != nil {
			return err
		}
		evt := events.BookingEvent{BookingID: bookingID, Type: events.BookingCancelled,
			PassengerName: b.PassengerName, Reason: reason, CorrelationID: b.CorrelationID}
		if err := writeOutbox(ctx, tx, events.TopicBookingEvents, key(bookingID), "BookingEvent", evt); err != nil {
			return err
		}
		s.log.Info("booking CANCELLED after payment failure", "bookingId", bookingID, "reason", reason)
		return nil
	})
	if err == nil {
		metrics.ObserveBookingTerminal(StatusCancelled, createdAt)
	}
	return err
}
