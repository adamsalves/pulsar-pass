package eventbus

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
)

// dlqMetrics holds the lazily created instruments: the package
// initializes before the binaries call metrics.Init, so instruments
// bind on first use to the provider in place at that moment — without
// Init they bind to the no-op default and cost nothing.
type dlqMetrics struct {
	once      sync.Once
	advisory  api.Int64Counter
	initError error
}

var dlqM dlqMetrics

// incDLQAdvisory counts one max-deliveries advisory, the signal that a
// message exhausted its redelivery budget and landed in the DLQ.
func incDLQAdvisory(stream, consumer string) {
	dlqM.once.Do(func() {
		meter := otel.Meter("pulsar-pass/eventbus")
		dlqM.advisory, dlqM.initError = meter.Int64Counter("pulsar_eventbus_dlq_advisories_total",
			api.WithDescription("Messages that exceeded max deliveries (DLQ advisories)"))
	})
	if dlqM.initError != nil || dlqM.advisory == nil {
		return
	}
	dlqM.advisory.Add(context.Background(), 1, api.WithAttributes(
		attribute.String("stream", stream),
		attribute.String("consumer", consumer),
	))
}
