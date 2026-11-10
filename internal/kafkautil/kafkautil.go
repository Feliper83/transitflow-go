// Package kafkautil wraps segmentio/kafka-go with the small amount of
// configuration the TransitFlow services share: a JSON writer and a grouped
// reader.
package kafkautil

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// NewWriter builds a Kafka writer that creates topics on demand and balances by
// message key, so records for the same booking land on the same partition and
// preserve per-booking ordering.
func NewWriter(brokers string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(splitBrokers(brokers)...),
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
		BatchTimeout:           200 * time.Millisecond,
	}
}

// NewReader builds a consumer-group reader for a single topic.
func NewReader(brokers, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     splitBrokers(brokers),
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
}

// Publish writes a single keyed message to a topic.
func Publish(ctx context.Context, w *kafka.Writer, topic, key string, value []byte) error {
	return w.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	})
}

func splitBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
