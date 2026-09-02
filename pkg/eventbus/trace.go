package eventbus

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CorrelationIDHeader carries the business correlation across service
// boundaries; it travels next to the W3C trace context injected by the
// bus, so a trace stays addressable by the id stamped on events.
const CorrelationIDHeader = "Correlation-Id"

// busTracing holds the lazily created tracer: the package initializes
// before the binaries run metrics.Init, so binding must happen on
// first use, against the global provider in place at that moment.
type busTracing struct {
	once   sync.Once
	tracer trace.Tracer
}

var busTrace busTracing

func (t *busTracing) get() trace.Tracer {
	t.once.Do(func() {
		t.tracer = otel.Tracer("pulsar-pass/eventbus")
	})
	return t.tracer
}

// headerCarrier adapts the message header map to the propagator
// interfaces so the W3C trace context rides the broker headers.
type headerCarrier map[string]string

func (c headerCarrier) Get(key string) string { return c[key] }

func (c headerCarrier) Set(key, value string) { c[key] = value }

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// injectTraceContext stamps the caller's span onto the outgoing
// message headers. Without an active span the propagator writes
// nothing and the message stays traceless.
func injectTraceContext(ctx context.Context, msg *Message) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(msg.Headers))
}

// publishSpan opens the producer-side span for one outgoing message.
func publishSpan(ctx context.Context, msg *Message) (context.Context, trace.Span) {
	ctx, span := busTrace.get().Start(ctx, "publish "+msg.Subject,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", msg.Subject),
			attribute.String("messaging.message.id", msg.ID),
		),
	)
	if cid := msg.Headers[CorrelationIDHeader]; cid != "" {
		span.SetAttributes(attribute.String("correlation_id", cid))
	}
	return ctx, span
}

// consumeSpan extracts the trace context from the delivered message
// and opens the consumer-side span linked to the producer. Handlers
// receive the span context, so their own work and every downstream
// publish join the same trace.
func consumeSpan(ctx context.Context, msg *Message) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier(msg.Headers))
	ctx, span := busTrace.get().Start(ctx, "consume "+msg.Subject,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", msg.Subject),
			attribute.String("messaging.message.id", msg.ID),
		),
	)
	if cid := msg.Headers[CorrelationIDHeader]; cid != "" {
		span.SetAttributes(attribute.String("correlation_id", cid))
	}
	return ctx, span
}
