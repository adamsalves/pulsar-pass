package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

func TestOutboxEnqueueAndAtomicWriteWithState(t *testing.T) {
	pool := coreTestDB(t)
	ctx := context.Background()
	outbox := postgres.NewOutbox(pool)
	reservations := postgres.NewReservations(pool)
	eventID := seedEvent(t, 5)

	manager := pgtx.NewManager(pool)
	res := newTestReservation(t, eventID)

	err := manager.WithinTx(ctx, func(txctx context.Context) error {
		if err := reservations.Create(txctx, res); err != nil {
			return err
		}
		return outbox.Enqueue(txctx, application.OutboxRecord{
			ID:            "aaaaaaaa-0000-4000-8000-000000000001",
			Subject:       "pulsarpass.reservations.events.ticket-reserved",
			EventType:     "ticket.reserved",
			EventVersion:  1,
			Source:        "pulsar-core",
			CorrelationID: res.ID,
			Payload:       []byte(`{"reservation_id":"` + res.ID + `"}`),
			OccurredAt:    time.Now().UTC(),
		})
	})
	if err != nil {
		t.Fatalf("transactional write error = %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE correlation_id = $1`, res.ID).Scan(&count); err != nil {
		t.Fatalf("count query error = %v", err)
	}
	if count != 1 {
		t.Fatalf("outbox rows = %d, want 1", count)
	}
}

func TestOutboxEnqueueRollsBackWithTransaction(t *testing.T) {
	pool := coreTestDB(t)
	ctx := context.Background()
	outbox := postgres.NewOutbox(pool)
	reservations := postgres.NewReservations(pool)
	eventID := seedEvent(t, 5)

	manager := pgtx.NewManager(pool)
	res := newTestReservation(t, eventID)

	_ = manager.WithinTx(ctx, func(txctx context.Context) error {
		_ = reservations.Create(txctx, res)
		_ = outbox.Enqueue(txctx, application.OutboxRecord{
			ID:        "aaaaaaaa-0000-4000-8000-000000000002",
			Subject:   "pulsarpass.reservations.events.ticket-reserved",
			EventType: "ticket.reserved",
			Source:    "pulsar-core",
			Payload:   []byte(`{}`),
		})
		return context.Canceled
	})

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE id = 'aaaaaaaa-0000-4000-8000-000000000002'`).Scan(&count); err != nil {
		t.Fatalf("count query error = %v", err)
	}
	if count != 0 {
		t.Fatalf("outbox rows = %d, want 0 (transaction must roll back)", count)
	}
}
