package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/adamsalves/pulsar-pass/internal/gateway"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
)

// captureBus records published commands so tests can assert what left
// the gateway.
type captureBus struct {
	mu       sync.Mutex
	messages []eventbus.Message
}

func (b *captureBus) Publish(_ context.Context, msg eventbus.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = append(b.messages, msg)
	return nil
}

func (b *captureBus) last(subject string) (eventbus.Message, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := len(b.messages) - 1; i >= 0; i-- {
		if b.messages[i].Subject == subject {
			return b.messages[i], true
		}
	}
	return eventbus.Message{}, false
}

func newTestServer(t *testing.T) (*httptest.Server, *captureBus) {
	t.Helper()
	bus := &captureBus{}
	api := httptest.NewServer(gateway.Routes(gateway.NewReservationHandler(bus, logger.New("test"), 8), logger.New("test")))
	t.Cleanup(api.Close)
	return api, bus
}

func post(t *testing.T, api *httptest.Server, path string, headers map[string]string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, api.URL+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := api.Client().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestCreateRejectsMissingUserId(t *testing.T) {
	api, bus := newTestServer(t)

	resp := post(t, api, "/v1/reservations", map[string]string{
		"Idempotency-Key": "idem-1",
	}, map[string]any{"event_id": "evt-1", "quantity": 1})

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (anonymous callers must not share the guest identity)", resp.StatusCode)
	}
	if n := len(bus.messages); n != 0 {
		t.Errorf("commands published = %d, want 0", n)
	}
}

func TestConfirmPaymentRejectsMissingUserId(t *testing.T) {
	api, bus := newTestServer(t)

	resp := post(t, api, "/v1/reservations/res-1/payment", map[string]string{
		"Idempotency-Key": "idem-1",
	}, map[string]any{"payment_method_token": "tok"})

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (payer identity must be explicit)", resp.StatusCode)
	}
	if n := len(bus.messages); n != 0 {
		t.Errorf("commands published = %d, want 0", n)
	}
}

func TestCreateAcceptsExplicitUser(t *testing.T) {
	api, bus := newTestServer(t)

	resp := post(t, api, "/v1/reservations", map[string]string{
		"Idempotency-Key": "idem-1",
		"X-User-Id":       "user-1",
	}, map[string]any{"event_id": "evt-1", "quantity": 1})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	var out struct {
		Status        string `json:"status"`
		ReservationID string `json:"reservation_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Status != "accepted" || out.ReservationID == "" {
		t.Fatalf("unexpected response %+v", out)
	}

	// The command must carry the explicit user identity, never a fallback.
	msg, ok := bus.last(envelope.SubjectReservationReserve)
	if !ok {
		t.Fatal("no reservation.reserve command published")
	}
	var cmd struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
		t.Fatalf("decode command: %v", err)
	}
	if cmd.UserID != "user-1" {
		t.Fatalf("command user_id = %q, want user-1", cmd.UserID)
	}
}

func TestConfirmPaymentAcceptsExplicitUser(t *testing.T) {
	api, bus := newTestServer(t)

	resp := post(t, api, "/v1/reservations/res-1/payment", map[string]string{
		"Idempotency-Key": "idem-1",
		"X-User-Id":       "user-2",
	}, map[string]any{"payment_method_token": "tok"})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	msg, ok := bus.last(envelope.SubjectPaymentProcess)
	if !ok {
		t.Fatal("no payment.process command published")
	}
	var cmd struct {
		ReservationID string `json:"reservation_id"`
		UserID        string `json:"user_id"`
	}
	if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
		t.Fatalf("decode command: %v", err)
	}
	if cmd.UserID != "user-2" || cmd.ReservationID != "res-1" {
		t.Fatalf("command = user %q / reservation %q, want user-2 / res-1", cmd.UserID, cmd.ReservationID)
	}
}
