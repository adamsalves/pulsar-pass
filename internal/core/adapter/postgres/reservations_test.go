package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/core/domain"
)

func TestReservationRoundtripAndOptimisticLock(t *testing.T) {
	pool := coreTestDB(t)
	ctx := context.Background()
	eventID := seedEvent(t, 5)
	repo := postgres.NewReservations(pool)

	now := time.Now().UTC()
	res := domain.NewReservation(domain.NewReservationInput{
		EventID:     eventID,
		UserID:      "user-1",
		Quantity:    2,
		AmountCents: 2000,
		TTL:         10 * time.Minute,
		Now:         now,
	})
	if err := repo.Create(ctx, res); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := repo.Get(ctx, res.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.ID != res.ID || got.Status != domain.ReservationStatusPending || got.AmountCents != 2000 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	if err := got.Confirm(now.Add(time.Minute)); err != nil {
		t.Fatalf("Confirm() error = %v", err)
	}
	if err := repo.Update(ctx, got); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if err := repo.Update(ctx, got); !errors.Is(err, domain.ErrConflict) {
		t.Errorf("stale Update() error = %v, want ErrConflict", err)
	}

	reloaded, err := repo.Get(ctx, res.ID)
	if err != nil {
		t.Fatalf("Get() after update error = %v", err)
	}
	if reloaded.Status != domain.ReservationStatusConfirmed || reloaded.Version != 2 {
		t.Fatalf("status = %q, version = %d; want CONFIRMED/2", reloaded.Status, reloaded.Version)
	}
}

func TestReservationDuplicateIDConflicts(t *testing.T) {
	pool := coreTestDB(t)
	ctx := context.Background()
	eventID := seedEvent(t, 5)
	repo := postgres.NewReservations(pool)

	now := time.Now().UTC()
	res := domain.NewReservation(domain.NewReservationInput{
		ID:       "11111111-2222-4333-8444-555555555555",
		EventID:  eventID,
		UserID:   "user-1",
		Quantity: 1,
		TTL:      10 * time.Minute,
		Now:      now,
	})
	if err := repo.Create(ctx, res); err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if err := repo.Create(ctx, res); !errors.Is(err, domain.ErrConflict) {
		t.Errorf("duplicate Create() error = %v, want ErrConflict", err)
	}
}

func TestGetMissingReservation(t *testing.T) {
	pool := coreTestDB(t)
	repo := postgres.NewReservations(pool)

	_, err := repo.Get(context.Background(), "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}
