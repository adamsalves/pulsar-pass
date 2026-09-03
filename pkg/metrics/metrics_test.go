package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

// TestInitWithOTLPEndpointInstallsTracerProvider: with the OTLP env
// set, Init installs a real tracer provider — spans get a valid trace
// context even before any collector is reachable (the gRPC exporter
// connects lazily).
func TestInitWithOTLPEndpointInstallsTracerProvider(t *testing.T) {
	t.Setenv(metrics.OTLPEndpointEnv, "127.0.0.1:14317")

	handler, shutdown, err := metrics.Init(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })
	if handler == nil || shutdown == nil {
		t.Fatal("Init() returned nil handler or shutdown")
	}

	_, span := otel.Tracer("test").Start(context.Background(), "probe")
	defer span.End()
	if !span.SpanContext().IsValid() {
		t.Fatal("span context invalid: the global tracer provider is still a no-op")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown error = %v", err)
	}
}

// TestSamplingRatioControlsRootSpans: the explicit ratio drives root
// span sampling (0 drops roots, 1 keeps them); unset keeps always
// sampling, and an invalid value fails the init loudly — a typo must
// not silently change the trace volume under load.
func TestSamplingRatioControlsRootSpans(t *testing.T) {
	t.Setenv(metrics.OTLPEndpointEnv, "127.0.0.1:14317")

	t.Run("ratio 0 drops root spans", func(t *testing.T) {
		t.Setenv(metrics.OTLPSampleRatioEnv, "0")
		if _, _, err := metrics.Init(context.Background(), "test-service"); err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		_, span := otel.Tracer("test").Start(context.Background(), "probe")
		defer span.End()
		if span.SpanContext().IsSampled() {
			t.Error("root span sampled with ratio 0")
		}
		_ = metrics.Shutdown(context.Background())
	})

	t.Run("ratio 1 keeps root spans", func(t *testing.T) {
		t.Setenv(metrics.OTLPSampleRatioEnv, "1")
		if _, _, err := metrics.Init(context.Background(), "test-service"); err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		_, span := otel.Tracer("test").Start(context.Background(), "probe")
		defer span.End()
		if !span.SpanContext().IsSampled() {
			t.Error("root span not sampled with ratio 1")
		}
		_ = metrics.Shutdown(context.Background())
	})

	t.Run("unset keeps always sampling", func(t *testing.T) {
		if _, _, err := metrics.Init(context.Background(), "test-service"); err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		_, span := otel.Tracer("test").Start(context.Background(), "probe")
		defer span.End()
		if !span.SpanContext().IsSampled() {
			t.Error("root span not sampled with the ratio unset")
		}
		_ = metrics.Shutdown(context.Background())
	})

	t.Run("invalid value fails fast", func(t *testing.T) {
		for _, raw := range []string{"0.5x", "NaN", "2", "-1"} {
			t.Setenv(metrics.OTLPSampleRatioEnv, raw)
			if _, _, err := metrics.Init(context.Background(), "test-service"); err == nil {
				t.Fatalf("Init() must reject the ratio %q", raw)
			}
			_ = metrics.Shutdown(context.Background())
		}
	})
}

// TestShutdownRunsRebindHooks: hooks registered with OnShutdown run on
// Shutdown — the mechanism that lets package-level lazy instrument
// bindings re-arm after the providers stop instead of staying pinned
// to the shut-down provider.
func TestShutdownRunsRebindHooks(t *testing.T) {
	var calls atomic.Int32
	metrics.OnShutdown(func() { calls.Add(1) })

	if _, _, err := metrics.Init(context.Background(), "test-service"); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if err := metrics.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("hook calls = %d, want 1 after Shutdown", calls.Load())
	}
}

// TestReinitRebindsLazyInstruments reproduces the binding-lazy trap of
// the service packages: an instrument created once via sync.Do binds to
// the provider in place at that moment. Without the OnShutdown reset,
// a second Init would install a fresh provider while the instrument
// kept writing into the shut-down one — the scrape of cycle 2 would
// read zero. With the reset, the once re-arms and the instrument
// rebinds, so the second scrape shows the value.
func TestReinitRebindsLazyInstruments(t *testing.T) {
	var (
		once    sync.Once
		counter api.Int64Counter
	)
	bind := func() {
		once.Do(func() {
			var err error
			counter, err = otel.Meter("rebind-test").Int64Counter("rebind_test_total")
			if err != nil {
				t.Errorf("counter: %v", err)
			}
		})
	}
	metrics.OnShutdown(func() {
		once = sync.Once{}
		counter = nil
	})

	// counterValue returns the sampled value of rebind_test_total from
	// the scrape, format-agnostically: the exporter may append scope
	// labels to the series line.
	counterValue := func(body string) string {
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "rebind_test_total{") || strings.HasPrefix(line, "rebind_test_total ") {
				if fields := strings.Fields(line); len(fields) >= 2 {
					return fields[len(fields)-1]
				}
			}
		}
		return ""
	}

	scrape := func() string {
		handler, _, err := metrics.Init(context.Background(), "rebind-test")
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		bind()
		if counter == nil {
			t.Fatal("lazy binding did not create the instrument")
		}
		counter.Add(context.Background(), 1)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		// Package-level Shutdown, not the returned closure: only it
		// clears the init guard and runs the rebind hooks — the
		// lifecycle production and tests go through.
		if err := metrics.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown error = %v", err)
		}
		return rec.Body.String()
	}

	for cycle := 1; cycle <= 2; cycle++ {
		body := scrape()
		if got := counterValue(body); got != "1" {
			t.Errorf("cycle %d: rebind_test_total = %q, want 1 — the instrument must rebind to the active provider, not write into the shut-down one", cycle, got)
		}
	}
}
