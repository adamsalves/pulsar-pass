package eventbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/metrics"
)

// TestDLQAdvisoryCountedByOwnerOnly: every service receives the DLQ
// advisory broadcast, so counting without ownership filtering
// multiplies the metric by the number of deployed services (found in
// the cycle 4 load run: 5x inflation). Only the consumer's owner may
// count its advisories; foreign ones are ignored.
func TestDLQAdvisoryCountedByOwnerOnly(t *testing.T) {
	handler, _, err := metrics.Init(context.Background(), "pulsar-eventbus-test")
	if err != nil {
		t.Fatalf("metrics.Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })

	url := startEmbeddedNATS(t)
	bus, err := eventbus.ConnectJetStream(context.Background(), eventbus.JetStreamConfig{
		URL: url,
		Streams: []eventbus.StreamSpec{
			{Name: "TEST", Subjects: []string{"test.>"}},
		},
		MaxDeliver: 2,
		AckWait:    50 * time.Millisecond,
	}, logger.New("test"))
	if err != nil {
		t.Fatalf("connect jetstream: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })

	if err := bus.Subscribe("test.poison", "owned-consumer", func(context.Context, eventbus.Message) error {
		return errors.New("always fails")
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), eventbus.Message{
		ID:      "poison-1",
		Subject: "test.poison",
		Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait for the two paced deliveries and the exhaustion advisory.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := scrapeCounter(t, handler, "pulsar_eventbus_dlq_advisories_total"); ok && v >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// A foreign consumer's advisory must not increment this process's
	// counter.
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect raw nats: %v", err)
	}
	defer nc.Close()
	foreign, _ := json.Marshal(map[string]string{
		"stream":   "TEST",
		"consumer": "some-other-service-consumer",
		"subject":  "pulsarpass.payments.commands.process",
	})
	if err := nc.Publish("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.TEST.some-other-service-consumer", foreign); err != nil {
		t.Fatalf("publish foreign advisory: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	if v := scrapeCounterOrZero(t, handler, "pulsar_eventbus_dlq_advisories_total"); v != 1 {
		t.Errorf("dlq advisories = %v, want 1 (owned counted once; foreign ignored)", v)
	}
}

func scrapeCounter(t *testing.T, h http.Handler, name string) (float64, bool) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, name+"{") {
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

func scrapeCounterOrZero(t *testing.T, h http.Handler, name string) float64 {
	t.Helper()
	v, _ := scrapeCounter(t, h, name)
	return v
}
