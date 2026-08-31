package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/payment"
	pgadapter "github.com/adamsalves/pulsar-pass/internal/payment/adapter/postgres"
)

func TestPaymentCreateAndUpdateStatus(t *testing.T) {
	pool := payTestDB(t)
	ctx := context.Background()
	repo := pgadapter.NewPayments(pool)

	reservationID := "22222222-3333-4444-8555-666666666666"
	seedContext(t, reservationID)

	p := &payment.Payment{
		ID:             "33333333-4444-4555-8666-777777777777",
		ReservationID:  reservationID,
		UserID:         "user-1",
		AmountCents:    2500,
		Currency:       "BRL",
		Status:         payment.PaymentStatusPending,
		IdempotencyKey: "idem-create-1",
	}
	if err := repo.Create(ctx, p); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := repo.UpdateStatus(ctx, p.ID, payment.PaymentStatusSucceeded, "gw-123", ""); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}

	var status, ref string
	if err := pool.QueryRow(ctx,
		`SELECT status, gateway_ref FROM payments WHERE id = $1`, p.ID).Scan(&status, &ref); err != nil {
		t.Fatalf("verify payment: %v", err)
	}
	if status != "SUCCEEDED" || ref != "gw-123" {
		t.Fatalf("status = %q, ref = %q; want SUCCEEDED/gw-123", status, ref)
	}
}

func TestContextUpsertAndGet(t *testing.T) {
	pool := payTestDB(t)
	ctx := context.Background()
	repo := pgadapter.NewContexts(pool)

	reservationID := "44444444-5555-4666-8777-888888888888"
	seedContext(t, reservationID)

	newExpiry := time.Now().UTC().Add(15 * time.Minute)
	if err := repo.Upsert(ctx, payment.ReservationContext{
		ReservationID: reservationID,
		UserID:        "user-2",
		AmountCents:   9999,
		Currency:      "BRL",
		ExpiresAt:     newExpiry,
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	got, err := repo.Get(ctx, reservationID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.UserID != "user-2" || got.AmountCents != 9999 {
		t.Fatalf("upsert mismatch: %+v", got)
	}

	if _, err := repo.Get(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, payment.ErrContextNotFound) {
		t.Errorf("missing context error = %v, want ErrContextNotFound", err)
	}
}

func TestPaymentOutboxEnqueue(t *testing.T) {
	pool := payTestDB(t)
	ctx := context.Background()
	outbox := pgadapter.NewOutbox(pool)

	rec := payment.OutboxRecord{
		ID:            "55555555-6666-4777-8888-999999999999",
		Subject:       "pulsarpass.payments.events.payment-succeeded",
		EventType:     "payment.succeeded",
		EventVersion:  1,
		Source:        "pulsar-payment",
		CorrelationID: "22222222-3333-4444-8555-666666666666",
		Payload:       []byte(`{"reservation_id":"22222222-3333-4444-8555-666666666666"}`),
		OccurredAt:    time.Now().UTC(),
	}
	if err := outbox.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE id = $1`, rec.ID).Scan(&count); err != nil {
		t.Fatalf("verify outbox row: %v", err)
	}
	if count != 1 {
		t.Fatalf("outbox rows = %d, want 1", count)
	}
}
