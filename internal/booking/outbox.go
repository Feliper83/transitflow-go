package booking

import (
	"context"
	"log/slog"
	"time"

	"github.com/Feliper83/transitflow-go/internal/kafkautil"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// OutboxRelay polls the outbox and publishes unsent events to Kafka, then marks
// them sent. At-least-once by design: a crash after publishing but before
// marking sent re-publishes, which downstream idempotent consumers tolerate.
type OutboxRelay struct {
	pool     *pgxpool.Pool
	writer   *kafka.Writer
	interval time.Duration
	log      *slog.Logger
}

// NewOutboxRelay builds an OutboxRelay.
func NewOutboxRelay(pool *pgxpool.Pool, writer *kafka.Writer, interval time.Duration, log *slog.Logger) *OutboxRelay {
	return &OutboxRelay{pool: pool, writer: writer, interval: interval, log: log}
}

// Run polls until the context is cancelled.
func (r *OutboxRelay) Run(ctx context.Context) {
	r.log.Info("outbox relay started", "intervalMs", r.interval.Milliseconds())
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.publishPending(ctx); err != nil {
				r.log.Error("outbox relay cycle failed", "error", err)
			}
		}
	}
}

type outboxRow struct {
	id        string
	topic     string
	key       string
	eventType string
	payload   []byte
}

func (r *OutboxRelay) publishPending(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `SELECT id, topic, message_key, event_type, payload
		FROM outbox_events WHERE sent = FALSE ORDER BY created_at ASC LIMIT 100`)
	if err != nil {
		return err
	}
	var pending []outboxRow
	for rows.Next() {
		var o outboxRow
		if err := rows.Scan(&o.id, &o.topic, &o.key, &o.eventType, &o.payload); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, o := range pending {
		if err := kafkautil.Publish(ctx, r.writer, o.topic, o.key, o.payload); err != nil {
			return err
		}
		if _, err := r.pool.Exec(ctx, `UPDATE outbox_events SET sent = TRUE WHERE id = $1`, o.id); err != nil {
			return err
		}
		r.log.Info("relayed outbox event", "id", o.id, "type", o.eventType, "topic", o.topic)
	}
	return nil
}
