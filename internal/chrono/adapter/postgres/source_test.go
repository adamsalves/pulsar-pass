package postgres_test

import (
	"context"
	"slices"
	"testing"

	chronoadapter "github.com/adamsalves/pulsar-pass/internal/chrono/adapter/postgres"
)

func TestFindExpiredReturnsOnlyOverduePending(t *testing.T) {
	pool := chronoTestDB(t)
	ctx := context.Background()
	eventID := seedChronoEvent(t)

	expired1 := seedChronoReservation(t, eventID, "user-1", "now() - interval '5 minutes'")
	expired2 := seedChronoReservation(t, eventID, "user-2", "now() - interval '1 minute'")
	active := seedChronoReservation(t, eventID, "user-3", "now() + interval '10 minutes'")

	source := chronoadapter.NewSource(pool)
	got, err := source.FindExpired(ctx, 100)
	if err != nil {
		t.Fatalf("FindExpired() error = %v", err)
	}

	ids := make([]string, 0, len(got))
	for _, e := range got {
		ids = append(ids, e.ReservationID)
	}
	if !slices.Contains(ids, expired1) || !slices.Contains(ids, expired2) {
		t.Fatalf("expired reservations missing from result: %v", ids)
	}
	if slices.Contains(ids, active) {
		t.Fatalf("active reservation must not be returned: %v", ids)
	}
}

func TestFindExpiredRespectsLimit(t *testing.T) {
	pool := chronoTestDB(t)
	ctx := context.Background()
	eventID := seedChronoEvent(t)

	seedChronoReservation(t, eventID, "user-l1", "now() - interval '2 minutes'")
	seedChronoReservation(t, eventID, "user-l2", "now() - interval '3 minutes'")

	source := chronoadapter.NewSource(pool)
	got, err := source.FindExpired(ctx, 1)
	if err != nil {
		t.Fatalf("FindExpired() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1 (LIMIT respected)", len(got))
	}
}
