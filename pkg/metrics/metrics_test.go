package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adamsalves/pulsar-pass/pkg/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
)

func TestInitServesScrapeWithInstrumentedValues(t *testing.T) {
	handler, shutdown, err := metrics.Init(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })
	if shutdown == nil {
		t.Fatal("Init() returned nil shutdown")
	}

	// An instrument created after Init must be exported by the
	// Prometheus endpoint; the default no-op provider would not.
	meter := otel.Meter("test")
	counter, err := meter.Int64Counter("test_requests_total")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	counter.Add(context.Background(), 3, api.WithAttributes(attribute.String("route", "create")))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"test_requests_total",
		`route="create"`,
		"go_", // Go runtime instrumentation
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q:\n%s", want, body)
		}
	}
	if !strings.Contains(body, "3") {
		t.Errorf("scrape missing counter value 3:\n%s", body)
	}
}

func TestInitTwiceFails(t *testing.T) {
	if _, _, err := metrics.Init(context.Background(), "test-service"); err != nil {
		t.Fatalf("first Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })

	if _, _, err := metrics.Init(context.Background(), "test-service-2"); err == nil {
		t.Fatal("second Init() must fail: the global provider and registry are singletons")
	}
}

func TestShutdownWithoutInitIsNoop(t *testing.T) {
	if err := metrics.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() without Init error = %v", err)
	}
}
