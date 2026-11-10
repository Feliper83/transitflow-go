package payment

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore is the pgx-backed Repository for payment-service.
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPgStore builds a PgStore.
func NewPgStore(pool *pgxpool.Pool) *PgStore { return &PgStore{pool: pool} }

const schemaSQL = `
CREATE TABLE IF NOT EXISTS payments (
    id           BIGSERIAL    PRIMARY KEY,
    booking_id   BIGINT       NOT NULL UNIQUE,
    amount       NUMERIC(12,2) NOT NULL,
    status       TEXT         NOT NULL,
    reason       TEXT,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);`

// Migrate creates the payments schema.
func (s *PgStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

func (s *PgStore) FindByBookingID(ctx context.Context, bookingID int64) (*Payment, error) {
	var p Payment
	var reason *string
	err := s.pool.QueryRow(ctx, `SELECT booking_id, amount, status, reason
		FROM payments WHERE booking_id = $1`, bookingID).
		Scan(&p.BookingID, &p.Amount, &p.Status, &reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if reason != nil {
		p.Reason = *reason
	}
	return &p, nil
}

// Save inserts a payment. The unique booking_id constraint is the idempotency
// guard; ON CONFLICT DO NOTHING keeps a racing duplicate from erroring.
func (s *PgStore) Save(ctx context.Context, p *Payment) error {
	var reason *string
	if p.Reason != "" {
		reason = &p.Reason
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO payments (booking_id, amount, status, reason)
		VALUES ($1, $2, $3, $4) ON CONFLICT (booking_id) DO NOTHING`,
		p.BookingID, p.Amount, p.Status, reason)
	return err
}
