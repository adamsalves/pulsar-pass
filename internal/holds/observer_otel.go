package holds

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
)

// OTelObserver adapts the hold store events to OpenTelemetry
// instruments, feeding the "acelerador degradado" signal: per-op
// outcome counters, a latency histogram and the breaker state gauge.
// It binds to the global meter provider on construction, so build it
// after metrics.Init.
type OTelObserver struct {
	ops        api.Int64Counter
	latency    api.Float64Histogram
	opened     api.Int64Counter
	recovered  api.Int64Counter
	stateGauge api.Int64Gauge
}

// NewOTelObserver creates the observer against the named service's
// meter from the global provider.
func NewOTelObserver(service string) (*OTelObserver, error) {
	meter := otel.Meter("pulsar-pass/holds")
	ops, err := meter.Int64Counter("pulsar_holds_ops_total",
		api.WithDescription("Hold store operations by op and outcome"))
	if err != nil {
		return nil, err
	}
	latency, err := meter.Float64Histogram("pulsar_holds_op_duration_seconds",
		api.WithDescription("Hold store operation latency; only attempts that reached Redis"),
		api.WithExplicitBucketBoundaries(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5))
	if err != nil {
		return nil, err
	}
	opened, err := meter.Int64Counter("pulsar_holds_breaker_opened_total",
		api.WithDescription("Times the hold circuit breaker opened"))
	if err != nil {
		return nil, err
	}
	recovered, err := meter.Int64Counter("pulsar_holds_breaker_recovered_total",
		api.WithDescription("Times the hold circuit breaker recovered"))
	if err != nil {
		return nil, err
	}
	stateGauge, err := meter.Int64Gauge("pulsar_holds_breaker_open",
		api.WithDescription("1 while the hold circuit breaker is open (fast path short-circuited)"))
	if err != nil {
		return nil, err
	}
	return &OTelObserver{
		ops:        ops,
		latency:    latency,
		opened:     opened,
		recovered:  recovered,
		stateGauge: stateGauge,
	}, nil
}

// ObserveOp implements Observer.
func (o *OTelObserver) ObserveOp(op string, latency time.Duration, outcome OpOutcome) {
	ctx := context.Background()
	o.ops.Add(ctx, 1, api.WithAttributes(
		attrOp(op), attrOutcome(outcome),
	))
	if outcome != OpShortCircuited {
		o.latency.Record(ctx, latency.Seconds(), api.WithAttributes(attrOp(op)))
	}
}

// BreakerOpened implements Observer.
func (o *OTelObserver) BreakerOpened() {
	o.opened.Add(context.Background(), 1)
	o.stateGauge.Record(context.Background(), 1)
}

// BreakerRecovered implements Observer.
func (o *OTelObserver) BreakerRecovered() {
	o.recovered.Add(context.Background(), 1)
	o.stateGauge.Record(context.Background(), 0)
}

func attrOp(op string) attribute.KeyValue { return attribute.String("op", op) }

func attrOutcome(outcome OpOutcome) attribute.KeyValue {
	name := "unknown"
	switch outcome {
	case OpSuccess:
		name = "success"
	case OpDegraded:
		name = "degraded"
	case OpShortCircuited:
		name = "short_circuited"
	}
	return attribute.String("outcome", name)
}
