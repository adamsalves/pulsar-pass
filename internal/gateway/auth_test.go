package gateway_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/adamsalves/pulsar-pass/internal/gateway"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
)

// TestAuthMiddlewareResolvesIdentity pins the auth contract on the real
// route chain: identity comes from the bearer token table resolved by
// the gateway — a missing header, a wrong scheme or an unknown token
// all get 401 before reaching any handler, and a valid token rides the
// context into the command payload.
func TestAuthMiddlewareResolvesIdentity(t *testing.T) {
	t.Run("missing header gets 401 with WWW-Authenticate", func(t *testing.T) {
		api, bus := newTestServer(t)
		resp := post(t, api, "/v1/reservations", map[string]string{
			"Idempotency-Key": "idem-1",
		}, map[string]any{"event_id": "evt-1", "quantity": 1})
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
		if got := resp.Header.Get("WWW-Authenticate"); got == "" {
			t.Error("WWW-Authenticate header missing on 401")
		}
		if n := len(bus.messages); n != 0 {
			t.Errorf("commands published = %d, want 0", n)
		}
	})

	t.Run("non-bearer scheme gets 401", func(t *testing.T) {
		api, bus := newTestServer(t)
		resp := post(t, api, "/v1/reservations", map[string]string{
			"Idempotency-Key": "idem-1",
			"Authorization":   "Basic " + testToken,
		}, map[string]any{"event_id": "evt-1", "quantity": 1})
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
		if n := len(bus.messages); n != 0 {
			t.Errorf("commands published = %d, want 0", n)
		}
	})

	t.Run("unknown token gets 401", func(t *testing.T) {
		api, bus := newTestServer(t)
		resp := post(t, api, "/v1/reservations", map[string]string{
			"Idempotency-Key": "idem-1",
			"Authorization":   "Bearer not-in-the-table",
		}, map[string]any{"event_id": "evt-1", "quantity": 1})
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
		if n := len(bus.messages); n != 0 {
			t.Errorf("commands published = %d, want 0", n)
		}
	})

	t.Run("bearer scheme is case-insensitive", func(t *testing.T) {
		api, bus := newTestServer(t)
		resp := post(t, api, "/v1/reservations", map[string]string{
			"Idempotency-Key": "idem-1",
			"Authorization":   "bEaReR " + testToken,
		}, map[string]any{"event_id": "evt-1", "quantity": 1})
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", resp.StatusCode)
		}
		if _, ok := bus.last(envelope.SubjectReservationReserve); !ok {
			t.Fatal("no reservation.reserve command published")
		}
	})
}

// TestAuthUserIDFromContext checks the accessor the handlers rely on,
// including the zero value for a context without the middleware.
func TestAuthUserIDFromContext(t *testing.T) {
	var got string
	probe := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = gateway.UserIDFrom(r.Context())
	})

	t.Run("resolves the token's user", func(t *testing.T) {
		handler := gateway.Auth(map[string]string{"tok-1": "user-1"})(probe)
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Authorization", "Bearer tok-1")
		handler.ServeHTTP(httptest.NewRecorder(), req)
		if got != "user-1" {
			t.Errorf("UserIDFrom = %q, want user-1", got)
		}
	})

	t.Run("empty without the middleware", func(t *testing.T) {
		got = ""
		probe.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
		if got != "" {
			t.Errorf("UserIDFrom = %q, want empty outside the Auth chain", got)
		}
	})
}
