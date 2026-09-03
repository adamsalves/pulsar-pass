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

type fakePayments struct {
	store map[string]*payment.Payment
	byKey map[string]string
}

func newFakePayments() *fakePayments {
	return &fakePayments{
		store: make(map[string]*payment.Payment),
		byKey: make(map[string]string),
	}
}

func (f *fakePayments) Create(_ context.Context, p *payment.Payment) error {
	if _, dup := f.byKey[p.IdempotencyKey]; dup {
		return payment.ErrDuplicatePayment
	}
	f.store[p.ID] = p
	f.byKey[p.IdempotencyKey] = p.ID
	return nil
}

func (f *fakePayments) GetByIdempotencyKey(_ context.Context, key string) (*payment.Payment, error) {
	id, ok := f.byKey[key]
	if !ok {
		return nil, payment.ErrPaymentNotFound
	}
	cp := *f.store[id]
	return &cp, nil
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

// fakeTx runs fn directly, counting how many units of work were opened.
type fakeTx struct{ opens int }

func (f *fakeTx) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	f.opens++
	return fn(ctx)
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

// flakyContexts fails the first N Gets with ErrContextNotFound — the
// signature of a payment command racing the ticket.reserved projection
// through the outbox relay.
type flakyContexts struct {
	*fakeContexts
	misses   int
	getCalls int
}

func (f *flakyContexts) Get(ctx context.Context, reservationID string) (payment.ReservationContext, error) {
	f.getCalls++
	if f.misses > 0 {
		f.misses--
		return payment.ReservationContext{}, payment.ErrContextNotFound
	}
	return f.fakeContexts.Get(ctx, reservationID)
}

// errContexts always fails with an infrastructure error, exercising the
// non-retryable branch of the context wait.
type errContexts struct{ err error }

func (f *errContexts) Upsert(_ context.Context, _ payment.ReservationContext) error { return f.err }

func (f *errContexts) Get(_ context.Context, _ string) (payment.ReservationContext, error) {
	return payment.ReservationContext{}, f.err
}

// countingSleeper replaces the default wait with an immediate return,
// counting the calls so budgets are asserted without real time.
type countingSleeper struct{ calls int }

func (s *countingSleeper) sleep(_ context.Context, _ time.Duration) error {
	s.calls++
	return nil
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
	svc := payment.NewProcessor(payments, newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	err := svc.Handle(context.Background(), payment.PaymentRequested{
		ReservationID: "res-1",
		UserID:        "user-1",
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
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
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
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(), &fakeOutbox{}, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-404", UserID: "user-1", Token: "tok"}, "idem-1")
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
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(testContext("res-1", now.Add(-time.Minute))), outbox, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if acquirer.calls != 0 {
		t.Errorf("acquirer called %d times, want 0 after window elapsed", acquirer.calls)
	}
	if len(outbox.records) != 1 || outbox.records[0].EventType != envelope.TypePaymentFailed {
		t.Fatalf("outbox = %+v, want payment.failed", outbox.records)
	}
}

func TestHandleRedeliveryResumesPendingCharge(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{ref: "sim-456"}
	payments := newFakePayments()
	tx := &fakeTx{}
	svc := payment.NewProcessor(payments, newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, tx, acquirer, &fakeClock{now: now}, slog.Default())

	// First attempt: charge approved, status committed, then the process
	// "crashes" before the outcome is delivered. Simulate by handling
	// once and wiping the outbox.
	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	outbox.records = nil

	// Redelivery with the same idempotency key must NOT create a second
	// payment nor charge again.
	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("redelivered Handle() error = %v", err)
	}
	if len(payments.store) != 1 {
		t.Fatalf("payments persisted = %d, want 1 (no duplicate attempt)", len(payments.store))
	}
	if acquirer.calls != 1 {
		t.Fatalf("acquirer calls = %d, want 1 (decided payments are never recharged)", acquirer.calls)
	}
	if len(outbox.records) != 1 || outbox.records[0].EventType != envelope.TypePaymentSucceeded {
		t.Fatalf("outbox = %+v, want outcome re-recorded as payment.succeeded", outbox.records)
	}
}

func TestHandleRedeliveryOfFailedPaymentRepublishesOutcome(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{err: errors.New("card declined")}
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	outbox.records = nil

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("redelivered Handle() error = %v", err)
	}
	if acquirer.calls != 1 {
		t.Fatalf("acquirer calls = %d, want 1", acquirer.calls)
	}
	if len(outbox.records) != 1 || outbox.records[0].EventType != envelope.TypePaymentFailed {
		t.Fatalf("outbox = %+v, want payment.failed re-recorded", outbox.records)
	}
}

func TestFinishWritesStatusAndOutboxInTheSameTransaction(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{ref: "sim-789"}
	payments := newFakePayments()
	tx := &fakeTx{}
	svc := payment.NewProcessor(payments, newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, tx, acquirer, &fakeClock{now: now}, slog.Default())

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if tx.opens != 1 {
		t.Fatalf("transaction opens = %d, want 1 (status + outbox atomic)", tx.opens)
	}
}

func TestHandleNonOwnerRejected(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{ref: "sim-123"}
	payments := newFakePayments()
	svc := payment.NewProcessor(payments, newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), outbox, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-2", Token: "tok"}, "idem-1")
	if !errors.Is(err, payment.ErrNotOwner) {
		t.Fatalf("error = %v, want ErrNotOwner", err)
	}
	if acquirer.calls != 0 {
		t.Errorf("acquirer called %d times, want 0 (impostor must not reach the gateway)", acquirer.calls)
	}
	if len(payments.store) != 0 {
		t.Errorf("payments persisted = %d, want 0 (rejection is side-effect free)", len(payments.store))
	}
	if len(outbox.records) != 0 {
		t.Errorf("outbox records = %d, want 0 (no payment.failed: the reservation must stay payable by the owner)", len(outbox.records))
	}
}

func TestHandleMissingUserRejected(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	acquirer := &fakeAcquirer{}
	svc := payment.NewProcessor(newFakePayments(), newFakeContexts(testContext("res-1", now.Add(5*time.Minute))), &fakeOutbox{}, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	// An anonymous command must not default to the reservation owner:
	// defaulting would silently charge whoever the projection names.
	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", Token: "tok"}, "idem-1"); err == nil {
		t.Fatal("Handle() error = nil, want rejection for missing user_id")
	}
	if acquirer.calls != 0 {
		t.Errorf("acquirer called %d times, want 0", acquirer.calls)
	}
}

func TestHandleContextWaitResolvesAfterProjectionLands(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{}
	acquirer := &fakeAcquirer{ref: "sim-wait"}
	inner := newFakeContexts(testContext("res-1", now.Add(5*time.Minute)))
	contexts := &flakyContexts{fakeContexts: inner, misses: 2}
	sleeper := &countingSleeper{}
	svc := payment.NewProcessor(newFakePayments(), contexts, outbox, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default(), payment.WithSleeper(sleeper.sleep))

	// The projection lands on the third read: the charge proceeds
	// instead of the error going back to the broker.
	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1"); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if acquirer.calls != 1 {
		t.Errorf("acquirer calls = %d, want 1 (wait resolved the race inline)", acquirer.calls)
	}
	if len(outbox.records) != 1 || outbox.records[0].EventType != envelope.TypePaymentSucceeded {
		t.Fatalf("outbox = %+v, want payment.succeeded", outbox.records)
	}
	if contexts.getCalls != 3 {
		t.Errorf("context reads = %d, want 3 (initial + 2 waits)", contexts.getCalls)
	}
	if sleeper.calls != 2 {
		t.Errorf("sleeper calls = %d, want 2", sleeper.calls)
	}
}

func TestHandleContextWaitExhaustsBudgetAndStaysRetryable(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	acquirer := &fakeAcquirer{}
	outbox := &fakeOutbox{}
	contexts := &flakyContexts{fakeContexts: newFakeContexts(), misses: 99}
	sleeper := &countingSleeper{}
	svc := payment.NewProcessor(newFakePayments(), contexts, outbox, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default(), payment.WithSleeper(sleeper.sleep))

	err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-404", UserID: "user-1", Token: "tok"}, "idem-1")
	if !errors.Is(err, payment.ErrContextNotFound) {
		t.Fatalf("error = %v, want ErrContextNotFound (retryable, broker redelivers)", err)
	}
	if acquirer.calls != 0 {
		t.Errorf("acquirer called %d times, want 0", acquirer.calls)
	}
	if len(outbox.records) != 0 {
		t.Errorf("outbox records = %d, want 0", len(outbox.records))
	}
	// Bounded budget: 3 waits × 500ms by default — exactly 1.5s inline,
	// leaving the pacing window of the broker as the outer bound.
	if contexts.getCalls != 4 {
		t.Errorf("context reads = %d, want 4 (initial + default 3 attempts)", contexts.getCalls)
	}
	if sleeper.calls != 3 {
		t.Errorf("sleeper calls = %d, want 3", sleeper.calls)
	}
}

func TestHandleContextWaitHonorsCustomBudget(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	acquirer := &fakeAcquirer{}
	contexts := &flakyContexts{fakeContexts: newFakeContexts(), misses: 99}
	sleeper := &countingSleeper{}
	svc := payment.NewProcessor(newFakePayments(), contexts, &fakeOutbox{}, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default(),
		payment.WithSleeper(sleeper.sleep), payment.WithContextWait(1, 100*time.Millisecond))

	if err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-404", UserID: "user-1", Token: "tok"}, "idem-1"); !errors.Is(err, payment.ErrContextNotFound) {
		t.Fatalf("error = %v, want ErrContextNotFound", err)
	}
	if sleeper.calls != 1 || contexts.getCalls != 2 {
		t.Errorf("budget = %d sleeps / %d reads, want 1 sleep / 2 reads", sleeper.calls, contexts.getCalls)
	}
}

func TestHandleContextWaitOtherErrorsAreNotRetryable(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	acquirer := &fakeAcquirer{}
	sleeper := &countingSleeper{}
	svc := payment.NewProcessor(newFakePayments(), &errContexts{err: errors.New("connection refused")}, &fakeOutbox{}, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default(), payment.WithSleeper(sleeper.sleep))

	err := svc.Handle(context.Background(), payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-1")
	if err == nil || errors.Is(err, payment.ErrContextNotFound) {
		t.Fatalf("error = %v, want the infrastructure error (not the context sentinel)", err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error = %v, want the repository error wrapped", err)
	}
	if sleeper.calls != 0 {
		t.Errorf("sleeper calls = %d, want 0 (only ErrContextNotFound is waited on)", sleeper.calls)
	}
}

func TestHandleContextWaitAbortsOnCancellation(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	acquirer := &fakeAcquirer{}
	contexts := &flakyContexts{fakeContexts: newFakeContexts(), misses: 99}
	svc := payment.NewProcessor(newFakePayments(), contexts, &fakeOutbox{}, &fakeTx{}, acquirer, &fakeClock{now: now}, slog.Default())

	// Shutdown mid-wait: the default sleeper aborts immediately and the
	// retryable sentinel goes back so the broker redelivers later.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := svc.Handle(ctx, payment.PaymentRequested{ReservationID: "res-404", UserID: "user-1", Token: "tok"}, "idem-1")
	if !errors.Is(err, payment.ErrContextNotFound) {
		t.Fatalf("error = %v, want ErrContextNotFound (shutdown keeps retryable semantics)", err)
	}
	if acquirer.calls != 0 {
		t.Errorf("acquirer called %d times, want 0", acquirer.calls)
	}
}
