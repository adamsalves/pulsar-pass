// Package postgres implements the horizon OutboxStore on top of
// PostgreSQL.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adamsalves/pulsar-pass/internal/horizon"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

// Store is the horizon.OutboxStore backed by PostgreSQL. FetchBatch is a
// plain read: crash-safety comes from publishing with the outbox event id
// as Nats-Msg-Id — a crash between publish and mark re-publishes later
// and the broker deduplicates.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds the outbox store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// FetchBatch returns the oldest unprocessed outbox records.
func (s *Store) FetchBatch(ctx context.Context, limit int) ([]horizon.OutboxRecord, error) {
	q := pgtx.QuerierFrom(ctx, s.pool)
	rows, err := q.Query(ctx, `
		SELECT id, subject, payload, correlation_id
		  FROM outbox_events
		 WHERE processed_at IS NULL
		 ORDER BY created_at
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []horizon.OutboxRecord
	for rows.Next() {
		var rec horizon.OutboxRecord
		var correlationID string
		if err := rows.Scan(&rec.ID, &rec.Subject, &rec.Payload, &correlationID); err != nil {
			return nil, err
		}
		if correlationID != "" {
			rec.Headers = map[string]string{"Correlation-Id": correlationID}
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// MarkProcessed flags records as relayed.
func (s *Store) MarkProcessed(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	q := pgtx.QuerierFrom(ctx, s.pool)
	_, err := q.Exec(ctx, `
		UPDATE outbox_events
		   SET processed_at = now()
		 WHERE id = ANY($1::uuid[])
		   AND processed_at IS NULL`, ids)
	return err
}
