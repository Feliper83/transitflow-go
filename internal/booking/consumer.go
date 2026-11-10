package booking

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/Feliper83/transitflow-go/internal/events"
	"github.com/segmentio/kafka-go"
)

// PaymentConsumer consumes payment.events and advances the SAGA to CONFIRMED or
// the compensation path.
type PaymentConsumer struct {
	reader       *kafka.Reader
	orchestrator *Orchestrator
	log          *slog.Logger
}

// NewPaymentConsumer builds the consumer.
func NewPaymentConsumer(reader *kafka.Reader, orchestrator *Orchestrator, log *slog.Logger) *PaymentConsumer {
	return &PaymentConsumer{reader: reader, orchestrator: orchestrator, log: log}
}

// Run consumes until the context is cancelled, committing offsets only after the
// SAGA step succeeds.
func (c *PaymentConsumer) Run(ctx context.Context) error {
	c.log.Info("payment-events consumer started", "topic", events.TopicPaymentEvents)
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			c.log.Error("fetch message failed", "error", err)
			return err
		}
		if err := c.handle(ctx, msg); err != nil {
			c.log.Error("processing failed, will retry", "error", err, "key", string(msg.Key))
			continue
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Error("commit failed", "error", err)
		}
	}
}

func (c *PaymentConsumer) handle(ctx context.Context, msg kafka.Message) error {
	var evt events.PaymentResultEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		c.log.Error("skipping unparseable payment event (poison message)", "error", err)
		return nil
	}
	c.log.Info("payment result received", "bookingId", evt.BookingID, "status", evt.Status)
	if evt.IsCompleted() {
		return c.orchestrator.Confirm(ctx, evt.BookingID)
	}
	return c.orchestrator.Compensate(ctx, evt.BookingID, evt.Reason)
}
