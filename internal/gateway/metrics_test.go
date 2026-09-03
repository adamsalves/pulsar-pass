package gateway_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adamsalves/pulsar-pass/pkg/metrics"
)

// TestMetricsMiddlewareRecordsRequests drives the routes with the
// metrics provider initialized and asserts the Prometheus scrape
// exposes request counts by route template and status, plus the
// duration histogram feeding the gateway p99.
func TestMetricsMiddlewareRecordsRequests(t *testing.T) {
	handler, _, err := metrics.Init(context.Background(), "pulsar-gateway-test")
	if err != nil {
		t.Fatalf("metrics.Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })

	api, _ := newTestServer(t)
	resp := post(t, api, "/v1/reservations", map[string]string{
		"Idempotency-Key": "idem-metrics",
		"Authorization":   "Bearer " + testToken,
	}, map[string]any{"event_id": "evt-1", "quantity": 1})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	resp = post(t, api, "/v1/reservations/res-1/payment", map[string]string{
		"Idempotency-Key": "idem-metrics",
	}, map[string]any{"payment_method_token": "tok"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (missing bearer token)", resp.StatusCode)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		"pulsar_gateway_http_requests_total",
		`http_route="/v1/reservations"`,
		`http_status_code="202"`,
		`http_route="/v1/reservations/{id}/payment"`,
		`http_status_code="401"`,
		"pulsar_gateway_http_request_duration_seconds_bucket",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q:\n%s", want, body)
		}
	}
	// The 401 on the payment route must not be counted under a concrete
	// reservation id (bounded cardinality).
	if strings.Contains(body, `http_route="/v1/reservations/res-1/payment"`) {
		t.Errorf("route label leaked a concrete id:\n%s", body)
	}
}
