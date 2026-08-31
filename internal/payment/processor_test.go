package payment_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/payment"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
)

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

type fakePayments struct{ store map[string]*payment.Payment }

func newFakePayments() *fakePayments {
	return &fakePayments{store: make(map[string]*payment.Payment)}
}

func (f *fakePayments) Create(_ context.Context, p *payment.Payment) error {
	f.store[p.ID] = p
	return nil
}

func (f *fakePayments) UpdateStatus(_ context.Context, id string, status payment.PaymentStatus, gatewayRef, failureReason string) error {
	p, ok := f.store[id]
	if !ok {
		return payment.ErrNotWired
	}
	p.Status = status
	p.GatewayRef = gatewayRef
	p.FailureReason = failureReason
	return nil
}

type fakeContexts struct {
	store map[string]payment.ReservationContext
}

func newFakeContexts(rcs ...payment.ReservationContext) *fakeContexts {
	m := make(map[string]payment.ReservationContext, len(rcs))
	for _, rc := range rcs {
		m[rc.ReservationID] = rc
	}
	return &fakeContexts{store: m}
}

func (f *fakeContexts) Upsert(_ context.Context, rc payment.ReservationContext) error {
	f.store[rc.ReservationID] = rc
	return nil
}

func (f *fakeContexts) Get(_ context.Context, reservationID string) (payment.ReservationContext, error) {
	rc, ok := f.store[reservationID]
	if !ok {
		return payment.ReservationContext{}, payment.ErrContextNotFound
	}
	return rc, nil
}

type fakeOutbox struct{ records []payment.OutboxRecord }

func (f *fakeOutbox) Enqueue(_ context.Context, records ...payment.OutboxRecord) error {
	f.records = append(f.records, records...)
	return nil
}

type fakeAcquirer struct {
	err   error
	ref   string
	calls int
}

func (f *fakeAcquirer) Charge(_ context.Context, _ payment.ChargeRequest) (payment.ChargeResult, error) {
	f.calls++
	if f.err != nil {
		return payment.ChargeResult{}, f.err
	}
	return payment.ChargeResult{GatewayRef: f.ref}, nil
}

func testContext(id string, expiresAt time.Time) payment.ReservationContext {
	return payment.ReservationContext{
		ReservationID: id,
		UserID:        "user-1",
		AmountCents:   2500,
		Currency:      "BRL",
		ExpiresAt:     expiresAt,
	}
}

func TestHandleApproved(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{ref: "sim-123"}
	payments := newFakePayments()
	svc := payment.NewProcessor(payments, newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, acquirer, &fakeClock{now: now}, slog.Default())

	err := svc.Handle(context.Background(), payment.PaymentRequested{
		ReservationID: "res-1",
		Token:         "tok-1",
	}, "idem-1")
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	found := false
	for _, p := range payments.store {
		found = true
		if p.Status != payment.PaymentStatusSucceeded {
			t.Errorf("status = %q, want SUCCEEDED", p.Status)
		}
		if p.AmountCents != 2500 {
			t.Errorf("amount = %d, want 2500 (from context)", p.AmountCents)
		}
		if p.GatewayRef != "sim-123" {
			t.Errorf("gateway_ref = %q, want sim-123", p.GatewayRef)
		}
	}
	if !found {
		t.Fatal("no payment persisted")
	}
	if len(outbox.records) != 1 || outbox.records[0].EventType != envelope.TypePaymentSucceeded {
		t.Fatalf("outbox = %+v, want one payment.succeeded record", outbox.records)
	}
}

func TestHandleDeclined(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{err: errors.New("card declined")}
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, acquirer, &fakeClock{now: now}, slog.Default())

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("Handle() error = %v (decline is a business outcome)", err)
	}
	if len(outbox.records) != 1 || outbox.records[0].EventType != envelope.TypePaymentFailed {
		t.Fatalf("outbox = %+v, want one payment.failed record", outbox.records)
	}
	if !strings.Contains(string(outbox.records[0].Payload), `"reason":"card declined"`) {
		t.Errorf("payload missing decline reason: %s", outbox.records[0].Payload)
	}
}

func TestHandleContextMissingIsRetryable(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	acquirer := &fakeAcquirer{}
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(), &fakeOutbox{}, acquirer, &fakeClock{now: now}, slog.Default())

	err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-404", Token: "tok"}, "idem-1")
	if !errors.Is(err, payment.ErrContextNotFound) {
		t.Fatalf("error = %v, want ErrContextNotFound", err)
	}
	if acquirer.calls != 0 {
		t.Errorf("acquirer called %d times, want 0", acquirer.calls)
	}
}

func TestHandleAfterWindowElapsed(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{}
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(testContext("res-1", now.Add(-time.Minute))), outbox, acquirer, &fakeClock{now: now}, slog.Default())

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if acquirer.calls != 0 {
		t.Errorf("acquirer called %d times, want 0 after window elapsed", acquirer.calls)
	}
	if len(outbox.records) != 1 || outbox.records[0].EventType != envelope.TypePaymentFailed {
		t.Fatalf("outbox = %+v, want payment.failed", outbox.records)
	}
}
