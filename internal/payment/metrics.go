package payment

import (
	"context"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
)

// chargeMetrics holds the lazily created instruments: the package
// initializes before the binaries call metrics.Init, so instruments
// bind on first use to the provider in place at that moment — without
// Init they bind to the no-op default and cost nothing.
type chargeMetrics struct {
	once    sync.Once
	charges api.Int64Counter
	err     error
}

var chMetrics chargeMetrics

func (m *chargeMetrics) counter() (api.Int64Counter, error) {
	m.once.Do(func() {
		meter := otel.Meter("pulsar-pass/payment")
		m.charges, m.err = meter.Int64Counter("pulsar_payment_charges_total",
			api.WithDescription("Acquirer charge outcomes"))
	})
	return m.charges, m.err
}

// recordChargeOutcome classifies one charge decision. The acquirer's
// decline reason is reduced to bounded classes to keep the label
// cardinality predictable; other rejections (window elapsed) get their
// own class.
func recordChargeOutcome(approved bool, reason string) {
	counter, err := chMetrics.counter()
	if err != nil {
		return
	}
	outcome := "approved"
	if !approved {
		switch {
		case strings.Contains(reason, "window elapsed"):
			outcome = "window_elapsed"
		case strings.Contains(reason, "card declined"):
			// The simulator declines with "card declined (forced by
			// token)"; real acquirers reuse the prefix.
			outcome = "declined"
		default:
			outcome = "acquirer_error"
		}
	}
	counter.Add(context.Background(), 1, api.WithAttributes(attribute.String("outcome", outcome)))
}
