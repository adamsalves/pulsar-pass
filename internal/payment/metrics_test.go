package payment_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/payment"
	"github.com/adamsalves/pulsar-pass/pkg/metrics"
)

// TestChargeOutcomeClassification drives the real Handle flow with the
// processor fakes and pins the reason → outcome classes on the
// Prometheus scrape: the simulated acquirer declines with
// "card declined (forced by token)", which must count as declined —
// not acquirer_error — or the decline rate undercounts.
func TestChargeOutcomeClassification(t *testing.T) {
	handler, _, err := metrics.Init(context.Background(), "pulsar-payment-test")
	if err != nil {
		t.Fatalf("metrics.Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	newProcessor := func(acquirer payment.Acquirer) *payment.Processor {
		return payment.NewProcessor(
			newFakePayments(),
			newFakeContexts(testContext("res-1", now.Add(5*time.Minute)), testContext("res-2", now.Add(5*time.Minute)), testContext("res-3", now.Add(-time.Minute))),
			&fakeOutbox{},
			&fakeTx{},
			acquirer,
			&fakeClock{now: now},
			slog.Default(),
		)
	}
	ctx := context.Background()
	if err := newProcessor(&fakeAcquirer{ref: "sim"}).Handle(ctx, payment.PaymentRequested{ReservationID: "res-1", UserID: "user-1", Token: "tok"}, "idem-approved"); err != nil {
		t.Fatalf("approved Handle() error = %v", err)
	}
	if err := newProcessor(&fakeAcquirer{err: errors.New("card declined (forced by token)")}).Handle(ctx, payment.PaymentRequested{ReservationID: "res-2", UserID: "user-1", Token: "tok"}, "idem-declined"); err != nil {
		t.Fatalf("declined Handle() error = %v", err)
	}
	if err := newProcessor(&fakeAcquirer{ref: "sim"}).Handle(ctx, payment.PaymentRequested{ReservationID: "res-3", UserID: "user-1", Token: "tok"}, "idem-window"); err != nil {
		t.Fatalf("window Handle() error = %v", err)
	}
	if err := newProcessor(&fakeAcquirer{err: errors.New("connection reset")}).Handle(ctx, payment.PaymentRequested{ReservationID: "res-2", UserID: "user-1", Token: "tok"}, "idem-error"); err != nil {
		t.Fatalf("acquirer error Handle() error = %v", err)
	}

	// Inline waits: the lazily bound instruments stay tied to the
	// provider of the first Init in the process, so the wait outcomes
	// are pinned in the same scrape as the charge outcomes.
	noSleep := func(context.Context, time.Duration) error { return nil }
	waitResolved := &flakyContexts{fakeContexts: newFakeContexts(testContext("res-4", now.Add(5*time.Minute))), misses: 1}
	if err := payment.NewProcessor(newFakePayments(), waitResolved, &fakeOutbox{}, &fakeTx{}, &fakeAcquirer{ref: "sim"}, &fakeClock{now: now}, slog.Default(), payment.WithSleeper(noSleep)).
		Handle(ctx, payment.PaymentRequested{ReservationID: "res-4", UserID: "user-1", Token: "tok"}, "idem-wait-resolved"); err != nil {
		t.Fatalf("resolved wait Handle() error = %v", err)
	}
	waitExhausted := &flakyContexts{fakeContexts: newFakeContexts(), misses: 99}
	err = payment.NewProcessor(newFakePayments(), waitExhausted, &fakeOutbox{}, &fakeTx{}, &fakeAcquirer{}, &fakeClock{now: now}, slog.Default(), payment.WithSleeper(noSleep)).
		Handle(ctx, payment.PaymentRequested{ReservationID: "res-404", UserID: "user-1", Token: "tok"}, "idem-wait-exhausted")
	if !errors.Is(err, payment.ErrContextNotFound) {
		t.Fatalf("exhausted wait Handle() error = %v, want ErrContextNotFound", err)
	}
	// Shutdown mid-wait: the aborted wait is counted in its own class —
	// it is neither resolved nor a budget exhaustion.
	waitAborted := &flakyContexts{fakeContexts: newFakeContexts(), misses: 99}
	abortedSleep := func(context.Context, time.Duration) error { return context.Canceled }
	err = payment.NewProcessor(newFakePayments(), waitAborted, &fakeOutbox{}, &fakeTx{}, &fakeAcquirer{}, &fakeClock{now: now}, slog.Default(), payment.WithSleeper(abortedSleep)).
		Handle(ctx, payment.PaymentRequested{ReservationID: "res-404", UserID: "user-1", Token: "tok"}, "idem-wait-aborted")
	if !errors.Is(err, payment.ErrContextNotFound) {
		t.Fatalf("aborted wait Handle() error = %v, want ErrContextNotFound", err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		"pulsar_payment_charges_total",
		`outcome="approved"`,
		`outcome="declined"`,
		`outcome="window_elapsed"`,
		`outcome="acquirer_error"`,
		"pulsar_payment_context_waits_total",
		`outcome="resolved"`,
		`outcome="exhausted"`,
		`outcome="aborted"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q:\n%s", want, body)
		}
	}
}
