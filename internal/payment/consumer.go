package payment

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"

	"github.com/Feliper83/transitflow-go/internal/events"
	"github.com/Feliper83/transitflow-go/internal/kafkautil"
	"github.com/segmentio/kafka-go"
)

// Consumer reads ProcessPaymentCommand messages, processes them idempotently,
// and publishes the resulting PaymentResultEvent.
type Consumer struct {
	reader *kafka.Reader
	writer *kafka.Writer
	svc    *Service
	log    *slog.Logger
}

// NewConsumer builds the payment consumer.
func NewConsumer(reader *kafka.Reader, writer *kafka.Writer, svc *Service, log *slog.Logger) *Consumer {
	return &Consumer{reader: reader, writer: writer, svc: svc, log: log}
}

// Run consumes until the context is cancelled. It commits the offset only after
// the result event is published, so a crash before publishing re-delivers the
// command — which the idempotent Service tolerates (at-least-once).
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("payment consumer started", "topic", events.TopicPaymentCommands)
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
			// Do not commit: the message will be redelivered and retried.
			c.log.Error("processing failed, will retry", "error", err, "key", string(msg.Key))
			continue
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Error("commit failed", "error", err)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var cmd events.ProcessPaymentCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		// A malformed message is a poison message: log and skip (commit) so it
		// does not block the partition. A production system routes it to a DLQ.
		c.log.Error("skipping unparseable command (poison message)", "error", err)
		return nil
	}
	result, err := c.svc.Process(ctx, cmd)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	key := strconv.FormatInt(result.BookingID, 10)
	if err := kafkautil.Publish(ctx, c.writer, events.TopicPaymentEvents, key, payload); err != nil {
		return err
	}
	c.log.Info("published payment result", "bookingId", result.BookingID, "status", result.Status)
	return nil
}
