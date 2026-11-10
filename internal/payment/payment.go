// Package payment implements payment-service: an idempotent Kafka consumer that
// processes ProcessPaymentCommand messages and emits PaymentResultEvent.
package payment

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Feliper83/transitflow-go/internal/events"
)

// Payment statuses persisted in the payments table.
const (
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

// Payment is a processed payment. BookingID is unique, which is what makes the
// consumer idempotent: a redelivered command for the same booking collides and
// is short-circuited instead of charging twice.
type Payment struct {
	BookingID int64
	Amount    float64
	Status    string
	Reason    string
}

// Repository is the persistence contract for payment-service.
type Repository interface {
	FindByBookingID(ctx context.Context, bookingID int64) (*Payment, error)
	Save(ctx context.Context, p *Payment) error
}

// Service holds payment business logic: idempotency and the decline threshold.
type Service struct {
	repo         Repository
	declineAbove float64
	log          *slog.Logger
}

// NewService builds a payment Service. Payments strictly above declineAbove are
// declined; all others complete.
func NewService(repo Repository, declineAbove float64, log *slog.Logger) *Service {
	return &Service{repo: repo, declineAbove: declineAbove, log: log}
}

// Process handles a payment command idempotently. If a payment already exists
// for the booking (duplicate delivery), the original outcome is returned
// unchanged and no second charge occurs.
func (s *Service) Process(ctx context.Context, cmd events.ProcessPaymentCommand) (events.PaymentResultEvent, error) {
	existing, err := s.repo.FindByBookingID(ctx, cmd.BookingID)
	if err != nil {
		return events.PaymentResultEvent{}, err
	}
	if existing != nil {
		s.log.Info("duplicate payment command ignored",
			"bookingId", cmd.BookingID, "status", existing.Status)
		return toEvent(existing, cmd.CorrelationID), nil
	}

	p := &Payment{BookingID: cmd.BookingID, Amount: cmd.Amount}
	if cmd.Amount > s.declineAbove {
		p.Status = StatusFailed
		p.Reason = fmt.Sprintf("Amount exceeds auto-approval limit of %.2f", s.declineAbove)
	} else {
		p.Status = StatusCompleted
	}
	if err := s.repo.Save(ctx, p); err != nil {
		return events.PaymentResultEvent{}, err
	}
	s.log.Info("payment processed", "bookingId", cmd.BookingID, "status", p.Status, "amount", cmd.Amount)
	return toEvent(p, cmd.CorrelationID), nil
}

func toEvent(p *Payment, correlationID string) events.PaymentResultEvent {
	if p.Status == StatusCompleted {
		return events.PaymentResultEvent{
			BookingID: p.BookingID, Amount: p.Amount,
			Status: events.PaymentCompleted, CorrelationID: correlationID,
		}
	}
	return events.PaymentResultEvent{
		BookingID: p.BookingID, Amount: p.Amount,
		Status: events.PaymentFailed, Reason: p.Reason, CorrelationID: correlationID,
	}
}
