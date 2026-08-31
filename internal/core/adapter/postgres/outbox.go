package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

// Outbox is the application.OutboxRepository backed by PostgreSQL. When
// called inside a transaction (via pgtx), rows are written atomically
// with the state change they describe.
type Outbox struct {
	pool *pgxpool.Pool
}

// NewOutbox builds the repository.
func NewOutbox(pool *pgxpool.Pool) *Outbox {
	return &Outbox{pool: pool}
}

// Enqueue writes one or more outbox records in a single round trip.
func (o *Outbox) Enqueue(ctx context.Context, records ...application.OutboxRecord) error {
	if len(records) == 0 {
		return nil
	}
	q := pgtx.QuerierFrom(ctx, o.pool)
	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(`
			INSERT INTO outbox_events
			    (id, subject, event_type, event_version, source,
			     correlation_id, causation_id, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)`,
			rec.ID, rec.Subject, rec.EventType, rec.EventVersion, rec.Source,
			rec.CorrelationID, rec.CausationID, string(rec.Payload), rec.OccurredAt)
	}
	br := q.SendBatch(ctx, batch)
	defer func() {
		_ = br.Close()
	}()
	for range records {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}
