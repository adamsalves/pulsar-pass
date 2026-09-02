package eventbus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
)

// TestJetStreamTraceContextPropagation drives publish → consume with a
// real tracer provider and asserts the consumer-side span joins the
// producer's trace through the broker headers: a gateway request and
// every downstream handler end up in the same trace.
func TestJetStreamTraceContextPropagation(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	})

	bus := newTestBus(t, nil)

	type delivered struct {
		mu   sync.Mutex
		ctx  context.Context
		got  bool
		done chan struct{}
	}
	d := &delivered{done: make(chan struct{})}
	if err := bus.Subscribe("test.trace", "trace-test", func(ctx context.Context, msg eventbus.Message) error {
		d.mu.Lock()
		d.ctx, d.got = ctx, true
		d.mu.Unlock()
		close(d.done)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	tracer := provider.Tracer("test-caller")
	cctx, caller := tracer.Start(context.Background(), "caller",
		trace.WithSpanKind(trace.SpanKindServer))
	err := bus.Publish(cctx, eventbus.Message{
		ID:      "trace-1",
		Subject: "test.trace",
		Payload: []byte(`{}`),
		Headers: map[string]string{eventbus.CorrelationIDHeader: "res-123"},
	})
	caller.End()
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-d.done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	d.mu.Lock()
	handlerCtx, got := d.ctx, d.got
	d.mu.Unlock()
	if !got {
		t.Fatal("delivery lost")
	}

	// The handler context must carry the caller's trace: an inner span
	// started by the consumer joins the same trace.
	_, inner := provider.Tracer("test-consumer").Start(handlerCtx, "inner-work")
	innerTraceID := inner.SpanContext().TraceID()
	inner.End()
	if innerTraceID != caller.SpanContext().TraceID() {
		t.Fatalf("trace id = %s, want caller trace %s (propagation failed)", innerTraceID, caller.SpanContext().TraceID())
	}

	spans := recorder.Ended()
	byName := make(map[string]sdktrace.ReadOnlySpan, len(spans))
	for _, s := range spans {
		byName[s.Name()] = s
	}
	pub, ok := byName["publish test.trace"]
	if !ok {
		t.Fatalf("publish span missing; ended = %v", spanNames(spans))
	}
	if pub.SpanKind() != trace.SpanKindProducer {
		t.Errorf("publish span kind = %s, want producer", pub.SpanKind())
	}
	cons, ok := byName["consume test.trace"]
	if !ok {
		t.Fatalf("consume span missing; ended = %v", spanNames(spans))
	}
	if cons.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("consume span kind = %s, want consumer", cons.SpanKind())
	}
	if cons.SpanContext().TraceID() != caller.SpanContext().TraceID() {
		t.Errorf("consume trace = %s, want caller trace", cons.SpanContext().TraceID())
	}
	if !hasAttr(cons, "correlation_id", "res-123") {
		t.Errorf("consume span missing correlation_id=res-123: %v", cons.Attributes())
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name())
	}
	return names
}

func hasAttr(s sdktrace.ReadOnlySpan, key, want string) bool {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key && kv.Value.AsString() == want {
			return true
		}
	}
	_ = attribute.String(key, want)
	return false
}
