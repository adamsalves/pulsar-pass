package eventbus

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CorrelationIDHeader carries the business correlation across service
// boundaries; it travels next to the W3C trace context injected by the
// bus, so a trace stays addressable by the id stamped on events.
const CorrelationIDHeader = "Correlation-Id"

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
// The tracer resolves against the global provider on every call:
// without metrics.Init it is a no-op, and tests may swap providers
// freely.
func publishSpan(ctx context.Context, msg *Message) (context.Context, trace.Span) {
	ctx, span := otel.Tracer("pulsar-pass/eventbus").Start(ctx, "publish "+msg.Subject,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(messagingAttributes(msg)...),
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
	ctx, span := otel.Tracer("pulsar-pass/eventbus").Start(ctx, "consume "+msg.Subject,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(messagingAttributes(msg)...),
	)
	if cid := msg.Headers[CorrelationIDHeader]; cid != "" {
		span.SetAttributes(attribute.String("correlation_id", cid))
	}
	return ctx, span
}

// messagingAttributes describes one message for both span kinds.
func messagingAttributes(msg *Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", msg.Subject),
		attribute.String("messaging.message.id", msg.ID),
	}
}
