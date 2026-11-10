package payment

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/Feliper83/transitflow-go/internal/events"
)

// fakeRepo tracks saves so tests can assert a duplicate is never charged again.
type fakeRepo struct {
	existing  *Payment
	saveCount int
}

func (f *fakeRepo) FindByBookingID(ctx context.Context, bookingID int64) (*Payment, error) {
	return f.existing, nil
}
func (f *fakeRepo) Save(ctx context.Context, p *Payment) error {
	f.saveCount++
	return nil
}

func testService(repo Repository) *Service {
	return NewService(repo, 500.00, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestApprovesPaymentWithinLimit(t *testing.T) {
	repo := &fakeRepo{}
	result, err := testService(repo).Process(context.Background(),
		events.ProcessPaymentCommand{BookingID: 1, Amount: 120.00, CorrelationID: "corr-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != events.PaymentCompleted {
		t.Fatalf("expected COMPLETED, got %s", result.Status)
	}
	if result.BookingID != 1 {
		t.Fatalf("expected bookingId 1, got %d", result.BookingID)
	}
	if repo.saveCount != 1 {
		t.Fatalf("expected one save, got %d", repo.saveCount)
	}
}

func TestDeclinesPaymentAboveLimit(t *testing.T) {
	repo := &fakeRepo{}
	result, err := testService(repo).Process(context.Background(),
		events.ProcessPaymentCommand{BookingID: 2, Amount: 600.00, CorrelationID: "corr-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != events.PaymentFailed {
		t.Fatalf("expected FAILED, got %s", result.Status)
	}
	if result.Reason == "" {
		t.Fatalf("expected a decline reason")
	}
}

func TestApprovesPaymentAtExactThreshold(t *testing.T) {
	repo := &fakeRepo{}
	// 500.00 is not strictly above the 500.00 threshold, so it must complete.
	result, err := testService(repo).Process(context.Background(),
		events.ProcessPaymentCommand{BookingID: 5, Amount: 500.00, CorrelationID: "corr-5"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != events.PaymentCompleted {
		t.Fatalf("expected COMPLETED at exact threshold, got %s", result.Status)
	}
}

func TestIsIdempotentForDuplicateCommand(t *testing.T) {
	repo := &fakeRepo{existing: &Payment{BookingID: 3, Amount: 120.00, Status: StatusCompleted}}
	result, err := testService(repo).Process(context.Background(),
		events.ProcessPaymentCommand{BookingID: 3, Amount: 120.00, CorrelationID: "corr-3"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != events.PaymentCompleted {
		t.Fatalf("expected COMPLETED from stored result, got %s", result.Status)
	}
	if repo.saveCount != 0 {
		t.Fatalf("duplicate command must not charge again; saves=%d", repo.saveCount)
	}
}
