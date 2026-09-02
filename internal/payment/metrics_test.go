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

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		"pulsar_payment_charges_total",
		`outcome="approved"`,
		`outcome="declined"`,
		`outcome="window_elapsed"`,
		`outcome="acquirer_error"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q:\n%s", want, body)
		}
	}
}
