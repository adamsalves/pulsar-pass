package core

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/internal/core/domain"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
)

// Minimal in-memory ports so the subscriber behavior — which outcomes
// are retried and which are acknowledged — is testable without
// Postgres.

type fakeReservations struct{}

func (fakeReservations) Get(context.Context, string) (*domain.Reservation, error) {
	return nil, domain.ErrNotFound
}

func (fakeReservations) Create(context.Context, *domain.Reservation) error { return nil }

func (fakeReservations) Update(context.Context, *domain.Reservation) error { return nil }

type fakeInventory struct{ reserveErr error }

func (f fakeInventory) Event(context.Context, string) (*domain.Event, error) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	return &domain.Event{
		ID:          "event-1",
		Name:        "Show",
		Venue:       "Arena",
		StartsAt:    base.Add(24 * time.Hour),
		SaleOpensAt: base.Add(-time.Hour),
		PriceCents:  1000,
		Capacity:    100,
	}, nil
}

func (f fakeInventory) ReserveCapacity(context.Context, string, int) error {
	return f.reserveErr
}

func (f fakeInventory) ReleaseCapacity(context.Context, string, int) error { return nil }

func (f fakeInventory) CommitSold(context.Context, string, int) error { return nil }

type fakeOutbox struct{}

func (fakeOutbox) Enqueue(context.Context, ...application.OutboxRecord) error { return nil }

type noopTx struct{}

func (noopTx) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

func newTestSubscriber(inventory fakeInventory) *Subscribers {
	service := application.NewReservationService(
		fakeReservations{},
		inventory,
		fakeOutbox{},
		noopTx{},
		application.SystemClock{},
		10*time.Minute,
		nil,
	)
	return NewSubscribers(service, slog.Default())
}

func reserveCommand() eventbus.Message {
	return eventbus.Message{Payload: []byte(`{"reservation_id":"11111111-1111-1111-1111-111111111111","event_id":"event-1","user_id":"u1","quantity":2}`)}
}

// TestOnReserveAcknowledgesTerminalRejections: sold-out is a terminal
// business outcome — the subscriber must acknowledge the command (nil
// error) so the delivery budget is not burned on a retry that can
// never succeed, keeping the DLQ for real poison.
func TestOnReserveAcknowledgesTerminalRejections(t *testing.T) {
	sub := newTestSubscriber(fakeInventory{reserveErr: domain.ErrSoldOut})

	if err := sub.onReserve(context.Background(), reserveCommand()); err != nil {
		t.Fatalf("onReserve error = %v, want nil (terminal rejection is acknowledged)", err)
	}
}

// TestOnReserveRetriesStoreFailures: non-terminal failures (store down)
// must keep the retryable contract — a non-nil error requests
// redelivery.
func TestOnReserveRetriesStoreFailures(t *testing.T) {
	sub := newTestSubscriber(fakeInventory{reserveErr: errors.New("connection reset")})

	if err := sub.onReserve(context.Background(), reserveCommand()); err == nil {
		t.Fatal("onReserve error = nil, want retryable error for store failure")
	}
}

// TestTerminalReserveRejectionClassification pins the terminal set:
// only sold-out and sale-not-open are acknowledged; everything else
// must retry.
func TestTerminalReserveRejectionClassification(t *testing.T) {
	if reason, ok := terminalReserveRejection(domain.ErrSoldOut); !ok || reason != "sold_out" {
		t.Errorf("sold_out = (%q, %v), want (sold_out, true)", reason, ok)
	}
	if reason, ok := terminalReserveRejection(domain.ErrSaleNotOpen); !ok || reason != "sale_not_open" {
		t.Errorf("sale_not_open = (%q, %v), want (sale_not_open, true)", reason, ok)
	}
	if _, ok := terminalReserveRejection(domain.ErrInvalidQuantity); ok {
		t.Error("invalid quantity must not be terminal (retryable)")
	}
	if _, ok := terminalReserveRejection(errors.New("connection reset")); ok {
		t.Error("store errors must not be terminal (retryable)")
	}
}
