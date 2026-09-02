package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// RequestID propagates or assigns an X-Request-Id to every request.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = uid.New()
		}
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDFrom extracts the request id stored by RequestID.
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// httpInstruments carries the lazily created OTel instruments: the
// package initializes before the binaries run metrics.Init, so binding
// must happen on first use, against the global provider in place at
// that moment.
type httpInstruments struct {
	once      sync.Once
	requests  api.Int64Counter
	duration  api.Float64Histogram
	initError error
}

var httpMetrics httpInstruments

func (i *httpInstruments) get() error {
	i.once.Do(func() {
		meter := otel.Meter("pulsar-pass/gateway")
		i.requests, i.initError = meter.Int64Counter(
			"pulsar_gateway_http_requests_total",
			api.WithDescription("HTTP requests processed by the gateway"))
		if i.initError != nil {
			return
		}
		i.duration, i.initError = meter.Float64Histogram(
			"pulsar_gateway_http_request_duration_seconds",
			api.WithDescription("HTTP request duration in seconds"),
			api.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
	})
	return i.initError
}

// Tracing opens a server span per request with the route template and
// the request id as attributes. The gateway is the root of each saga's
// trace by design: incoming W3C context is not extracted, because no
// upstream service currently calls the gateway over HTTP — all
// inter-service traffic rides the bus, which propagates. Revisit when
// an HTTP caller chain exists. The tracer resolves against the global
// provider on every request: without metrics.Init it is a no-op, and
// tests may swap providers freely.
func Tracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := routeTemplate(r)
		spanCtx, span := otel.Tracer("pulsar-pass/gateway").Start(r.Context(), "HTTP "+r.Method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.route", route),
				attribute.String("http.method", r.Method),
				attribute.String("http.request_id", RequestIDFrom(r.Context())),
			),
		)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(spanCtx))
		span.SetAttributes(attribute.Int("http.status_code", rec.status))
		span.End()
	})
}

// Metrics records request counts by route template and status plus the
// duration histogram feeding the gateway p99. It binds to the global
// OTel provider on first request; without metrics.Init it is a no-op.
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if err := httpMetrics.get(); err == nil {
			attrs := api.WithAttributes(
				attribute.String("http.route", routeTemplate(r)),
				attribute.String("http.method", r.Method),
				attribute.Int("http.status_code", rec.status),
			)
			httpMetrics.requests.Add(r.Context(), 1, attrs)
			httpMetrics.duration.Record(r.Context(), time.Since(start).Seconds(), attrs)
		}
	})
}

// routeTemplate reduces the concrete path to the route pattern so the
// cardinality of the route label stays bounded.
func routeTemplate(r *http.Request) string {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/v1/reservations/") && strings.HasSuffix(path, "/payment"):
		return "/v1/reservations/{id}/payment"
	case strings.HasPrefix(path, "/v1/reservations/"):
		return "/v1/reservations/{id}"
	case path == "/v1/reservations":
		return "/v1/reservations"
	default:
		return "unmatched"
	}
}

// Logging records method, path, status and duration of each request.
func Logging(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			log.Info("http request",
				"request_id", RequestIDFrom(r.Context()),
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration", time.Since(start).String(),
			)
		})
	}
}

// Recover converts panics into 500 responses instead of crashing the
// server.
func Recover(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic recovered",
						"request_id", RequestIDFrom(r.Context()),
						"panic", rec,
					)
					writeError(w, http.StatusInternalServerError, "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
