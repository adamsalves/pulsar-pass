package gateway_test

import (
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestTracingMiddlewareOpensServerSpan: every request gets a server
// span with the route template, request id and status, so the gateway
// is the trace root for the saga it kicks off.
func TestTracingMiddlewareOpensServerSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	})

	api, _ := newTestServer(t)
	resp := post(t, api, "/v1/reservations", map[string]string{
		"Idempotency-Key": "idem-trace",
		"X-User-Id":       "user-trace",
		"X-Request-Id":    "req-trace-1",
	}, map[string]any{"event_id": "evt-1", "quantity": 1})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	spans := recorder.Ended()
	var server *sdktrace.ReadOnlySpan
	for i, s := range spans {
		if s.SpanKind() == trace.SpanKindServer {
			server = &spans[i]
		}
	}
	if server == nil {
		t.Fatalf("no server span recorded; spans = %v", func() []string {
			names := make([]string, 0, len(spans))
			for _, s := range spans {
				names = append(names, s.Name())
			}
			return names
		}())
	}
	assertAttr(t, (*server).Attributes(), "http.route", "/v1/reservations")
	assertAttr(t, (*server).Attributes(), "http.request_id", "req-trace-1")
}

func assertAttr(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, kv := range attrs {
		if string(kv.Key) == key && kv.Value.AsString() == want {
			return
		}
	}
	t.Errorf("attribute %s=%q missing in %v", key, want, attrs)
}
