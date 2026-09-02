// Package metrics boots the OpenTelemetry meter provider for one
// service: a Prometheus exporter served by the health server under
// /metrics, Go runtime instrumentation and a no-op default until Init
// runs, so instrumented code paths cost nothing when metrics are off
// (tests, tools).
package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"

	"github.com/adamsalves/pulsar-pass/pkg/version"
)

var (
	initMu sync.Mutex
	// shutdown stops the provider installed by Init; nil until then.
	shutdown func(context.Context) error
)

// ErrAlreadyInitialized guards double Init in one process: the global
// meter provider and the exporter registry can only be set once.
var ErrAlreadyInitialized = errors.New("metrics already initialized in this process")

// Init installs the global meter provider backed by a Prometheus
// exporter and starts Go runtime instrumentation. The returned handler
// serves the metrics endpoint; the returned shutdown flushes and
// stops the provider. Init must run before service components create
// their instruments.
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

	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(0)); err != nil {
		// Roll the partial init back: without this, the global provider
		// would stay installed but untracked by Shutdown.
		_ = provider.Shutdown(context.Background())
		otel.SetMeterProvider(noop.NewMeterProvider())
		return nil, nil, fmt.Errorf("start runtime instrumentation: %w", err)
	}

	shutdown = provider.Shutdown
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), shutdown, nil
}

// Shutdown flushes and stops the provider installed by Init. Safe to
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
