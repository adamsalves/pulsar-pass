package holds_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/metrics"

	"github.com/adamsalves/pulsar-pass/internal/holds"
)

// TestOTelObserverExportsScrape wires the OTel observer with the
// metrics provider initialized and asserts the Prometheus scrape
// carries the degradation signals: per-op outcomes, breaker state and
// transitions.
func TestOTelObserverExportsScrape(t *testing.T) {
	handler, _, err := metrics.Init(context.Background(), "pulsar-holds-test")
	if err != nil {
		t.Fatalf("metrics.Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })

	obs, err := holds.NewOTelObserver("pulsar-holds-test")
	if err != nil {
		t.Fatalf("NewOTelObserver() error = %v", err)
	}

	obs.ObserveOp(holds.OpSet, 2*time.Millisecond, holds.OpDegraded)
	obs.ObserveOp(holds.OpSet, time.Millisecond, holds.OpShortCircuited)
	obs.BreakerOpened()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		"pulsar_holds_ops_total",
		`op="set"`,
		`outcome="degraded"`,
		`outcome="short_circuited"`,
		"pulsar_holds_op_duration_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q:\n%s", want, body)
		}
	}
	if v, ok := metricValue(body, "pulsar_holds_breaker_opened_total"); !ok || v != 1 {
		t.Errorf("breaker_opened = %v (found %v), want 1", v, ok)
	}
	if v, ok := metricValue(body, "pulsar_holds_breaker_open"); !ok || v != 1 {
		t.Errorf("breaker_open = %v (found %v), want 1", v, ok)
	}

	obs.BreakerRecovered()
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body = rec.Body.String()
	if v, ok := metricValue(body, "pulsar_holds_breaker_recovered_total"); !ok || v != 1 {
		t.Errorf("breaker_recovered = %v (found %v), want 1", v, ok)
	}
	if v, ok := metricValue(body, "pulsar_holds_breaker_open"); ok && v != 0 {
		t.Errorf("breaker_open = %v after recovery, want 0", v)
	}
}

// metricValue extracts the exported value of one metric family from a
// Prometheus text scrape, ignoring label sets.
func metricValue(body, name string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			var v float64
			if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err != nil {
				continue
			}
			return v, true
		}
	}
	return 0, false
}
