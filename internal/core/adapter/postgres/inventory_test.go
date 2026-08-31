package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/adamsalves/pulsar-pass/internal/core/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/core/domain"
)

func TestReserveCapacityZeroOverbookingUnderConcurrency(t *testing.T) {
	pool := coreTestDB(t)
	ctx := context.Background()
	eventID := seedEvent(t, 5)
	inv := postgres.NewInventory(pool)

	const workers = 10
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- inv.ReserveCapacity(ctx, eventID, 1)
		}()
	}
	wg.Wait()
	close(errs)

	var approved, soldOut int
	for err := range errs {
		switch {
		case err == nil:
			approved++
		case errors.Is(err, domain.ErrSoldOut):
			soldOut++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if approved != 5 || soldOut != 5 {
		t.Fatalf("approved = %d, soldOut = %d; want exactly 5/5 (zero overbooking)", approved, soldOut)
	}
}

func TestReserveCapacityUnknownEvent(t *testing.T) {
	pool := coreTestDB(t)
	inv := postgres.NewInventory(pool)

	err := inv.ReserveCapacity(context.Background(), "00000000-0000-0000-0000-000000000000", 1)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestEventRoundtrip(t *testing.T) {
	pool := coreTestDB(t)
	eventID := seedEvent(t, 5)
	inv := postgres.NewInventory(pool)

	e, err := inv.Event(context.Background(), eventID)
	if err != nil {
		t.Fatalf("Event() error = %v", err)
	}
	if e.ID != eventID || e.Capacity != 5 || e.PriceCents != 1000 || e.Name != "Show Teste" {
		t.Errorf("roundtrip mismatch: %+v", e)
	}

	_, err = inv.Event(context.Background(), "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("missing event error = %v, want ErrNotFound", err)
	}
}

func TestReleaseAndCommitSold(t *testing.T) {
	pool := coreTestDB(t)
	ctx := context.Background()
	eventID := seedEvent(t, 5)
	inv := postgres.NewInventory(pool)

	if err := inv.ReserveCapacity(ctx, eventID, 2); err != nil {
		t.Fatalf("ReserveCapacity() error = %v", err)
	}
	if err := inv.CommitSold(ctx, eventID, 1); err != nil {
		t.Fatalf("CommitSold() error = %v", err)
	}
	if err := inv.ReleaseCapacity(ctx, eventID, 1); err != nil {
		t.Fatalf("ReleaseCapacity() error = %v", err)
	}
	e, err := inv.Event(ctx, eventID)
	if err != nil {
		t.Fatalf("Event() error = %v", err)
	}
	if e.SoldCount != 1 || e.ReservedCount != 0 {
		t.Fatalf("sold = %d, reserved = %d; want 1/0", e.SoldCount, e.ReservedCount)
	}
	if err := inv.CommitSold(ctx, eventID, 5); !errors.Is(err, domain.ErrNotEnoughReserved) {
		t.Errorf("error = %v, want ErrNotEnoughReserved", err)
	}
}
