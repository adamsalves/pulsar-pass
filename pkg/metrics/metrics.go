// Package metrics boots the observability providers for one service:
// a Prometheus exporter served by the health server under /metrics,
// Go runtime instrumentation and — when OTEL_EXPORTER_OTLP_ENDPOINT
// is set — an OpenTelemetry tracer provider shipping spans over OTLP.
// Everything binds to the global providers and stays a no-op until
// Init runs, so instrumented code paths cost nothing when metrics are
// off (tests, tools).
package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/adamsalves/pulsar-pass/pkg/version"
)

// OTLPEndpointEnv is the standard OTLP endpoint variable: when set,
// Init installs a tracer provider shipping spans to that collector.
const OTLPEndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"

var (
	initMu sync.Mutex
	// shutdown stops the providers installed by Init; nil until then.
	shutdown func(context.Context) error
)

// ErrAlreadyInitialized guards double Init in one process: the global
// providers and the exporter registry can only be set once.
var ErrAlreadyInitialized = errors.New("metrics already initialized in this process")

// Init installs the global providers: the meter provider backed by a
// Prometheus exporter plus Go runtime instrumentation, and — when the
// OTLP endpoint env is set — a tracer provider with parent-based
// always sampling. The returned handler serves the metrics endpoint;
// the returned shutdown flushes and stops both providers. Init must
// run before service components create their instruments and tracers.
func Init(ctx context.Context, service string) (http.Handler, func(context.Context) error, error) {
	initMu.Lock()
	defer initMu.Unlock()
	if shutdown != nil {
		return nil, nil, ErrAlreadyInitialized
	}

	registry := prom.NewRegistry()
	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(registry))
	if err != nil {
		return nil, nil, fmt.Errorf("create prometheus exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(service), semconv.ServiceVersion(version.Version)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build resource: %w", err)
	}

	provider := metric.NewMeterProvider(
		metric.WithReader(exporter),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	tracerShutdown := func(context.Context) error { return nil }
	if ep := os.Getenv(OTLPEndpointEnv); ep != "" {
		ts, err := initTraces(ctx, ep, res)
		if err != nil {
			_ = provider.Shutdown(context.Background())
			otel.SetMeterProvider(metricnoop.NewMeterProvider())
			return nil, nil, err
		}
		tracerShutdown = ts
	} else {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(0)); err != nil {
		// Roll the partial init back: without this, the global providers
		// would stay installed but untracked by Shutdown.
		_ = provider.Shutdown(context.Background())
		_ = tracerShutdown(context.Background())
		otel.SetMeterProvider(metricnoop.NewMeterProvider())
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		return nil, nil, fmt.Errorf("start runtime instrumentation: %w", err)
	}

	shutdown = func(ctx context.Context) error {
		return errors.Join(tracerShutdown(ctx), provider.Shutdown(ctx))
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), shutdown, nil
}

// initTraces installs the OTLP tracer provider. The gRPC exporter
// connects lazily, so a collector that is not up yet only delays span
// delivery instead of failing the process. TLS collectors are out of
// scope for the local/compose topology of this cycle: both endpoint
// schemes dial plaintext, and real transport security joins the
// production wiring (Ciclo 6).
func initTraces(ctx context.Context, endpoint string, res *resource.Resource) (func(context.Context) error, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(stripEndpointScheme(endpoint)),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// stripEndpointScheme tolerates the standard OTLP env format
// (http://collector:4317); the gRPC exporter wants host:port.
func stripEndpointScheme(endpoint string) string {
	for _, prefix := range []string{"http://", "https://"} {
		if trimmed, ok := strings.CutPrefix(endpoint, prefix); ok {
			return trimmed
		}
	}
	return endpoint
}

// Shutdown flushes and stops the providers installed by Init. Safe to
// call when metrics were never initialized.
func Shutdown(ctx context.Context) error {
	initMu.Lock()
	sd := shutdown
	shutdown = nil
	initMu.Unlock()
	if sd == nil {
		return nil
	}
	return sd(ctx)
}
