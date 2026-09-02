package core

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"

	"github.com/adamsalves/pulsar-pass/internal/core/domain"
)

// useCaseMetrics holds the lazily created instruments. The package
// initializes before the binaries call metrics.Init, so instruments
// must bind on first use against the provider in place at that moment
// — without Init they bind to the no-op default and cost nothing.
type useCaseMetrics struct {
	once         sync.Once
	reservations api.Int64Counter
	err          error
}

var ucMetrics useCaseMetrics

func (m *useCaseMetrics) counter() (api.Int64Counter, error) {
	m.once.Do(func() {
		meter := otel.Meter("pulsar-pass/core")
		m.reservations, m.err = meter.Int64Counter("pulsar_core_reservations_total",
			api.WithDescription("Reservation use case outcomes"))
	})
	return m.reservations, m.err
}

// recordReserveOutcome classifies the Reserve result. Sold-out is the
// flash-sale signal: it feeds the "taxa de rejeição por esgotamento"
// alert from the observability blueprint.
func recordReserveOutcome(err error) {
	outcome := "created"
	switch {
	case err == nil:
	case errors.Is(err, domain.ErrSoldOut):
		outcome = "sold_out"
	case errors.Is(err, domain.ErrSaleNotOpen):
		outcome = "sale_not_open"
	case errors.Is(err, domain.ErrInvalidQuantity), errors.Is(err, domain.ErrInvalidID):
		outcome = "invalid"
	default:
		outcome = "error"
	}
	record("reserve", outcome)
}

// recordTerminalOutcome classifies a terminal use case (confirm, fail,
// expire): success covers first application and the idempotent replay
// guard keeps replays off the error path.
func recordTerminalOutcome(op string, err error) {
	outcome := "applied"
	switch {
	case err == nil:
	case errors.Is(err, domain.ErrInvalidTransition):
		outcome = "rejected"
	default:
		outcome = "error"
	}
	record(op, outcome)
}

func record(op, outcome string) {
	counter, err := ucMetrics.counter()
	if err != nil {
		return
	}
	counter.Add(context.Background(), 1, api.WithAttributes(
		attribute.String("op", op),
		attribute.String("outcome", outcome),
	))
}
