package postgres_test

import (
	"context"
	"testing"

	horizonadapter "github.com/adamsalves/pulsar-pass/internal/horizon/adapter/postgres"
)

func TestFetchBatchAndMarkProcessed(t *testing.T) {
	pool := horizonTestDB(t)
	ctx := context.Background()
	store := horizonadapter.NewStore(pool)

	id1 := "aaaaaaaa-0000-4000-8000-00000000a001"
	id2 := "aaaaaaaa-0000-4000-8000-00000000a002"
	seedOutboxRow(t, id1, "pulsarpass.reservations.events.ticket-reserved")
	seedOutboxRow(t, id2, "pulsarpass.reservations.events.ticket-released")

	batch, err := store.FetchBatch(ctx, 10)
	if err != nil {
		t.Fatalf("FetchBatch() error = %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch size = %d, want 2", len(batch))
	}
	for _, rec := range batch {
		if rec.Headers["Correlation-Id"] != rec.ID {
			t.Errorf("record %s missing correlation header", rec.ID)
		}
		if rec.Subject == "" {
			t.Errorf("record %s missing subject", rec.ID)
		}
	}

	if err := store.MarkProcessed(ctx, id1, id2); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}

	batch, err = store.FetchBatch(ctx, 10)
	if err != nil {
		t.Fatalf("FetchBatch() after mark error = %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("batch size = %d, want 0 after mark", len(batch))
	}
}
