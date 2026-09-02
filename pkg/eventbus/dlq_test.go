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

// TestDLQAdvisories pins the two layers that keep the DLQ metric true:
//
//   - Ownership (cycle 4): the advisory broadcast reaches every service,
//     so only the consumer's owner may count it — otherwise the metric
//     multiplies by the number of deployed services (5x inflation was
//     measured in the cycle 4 load run).
//   - Queue group (cycle 6): every replica of a service receives the
//     broadcast too, and all replicas own the same durable, so the
//     owner's own count would multiply by its replica count. With the
//     service's advisory queue group exactly one replica delivers and
//     exactly one process counts.
func TestDLQAdvisories(t *testing.T) {
	handler, _, err := metrics.Init(context.Background(), "pulsar-eventbus-test")
	if err != nil {
		t.Fatalf("metrics.Init() error = %v", err)
	}
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()) })

	url := startEmbeddedNATS(t)

	t.Run("owned advisory counted once, foreign ignored", func(t *testing.T) {
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
			if v, ok := scrapeCounter(t, handler, "owned-consumer"); ok && v >= 1 {
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
		foreign, _ := json.Marshal(map[string]any{
			"stream":     "TEST",
			"consumer":   "some-other-service-consumer",
			"stream_seq": 42,
			"deliveries": 10,
		})
		if err := nc.Publish("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.TEST.some-other-service-consumer", foreign); err != nil {
			t.Fatalf("publish foreign advisory: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		if v := scrapeCounterOrZero(t, handler, "owned-consumer"); v != 1 {
			t.Errorf("dlq advisories = %v, want 1 (owned counted once; foreign ignored)", v)
		}
	})

	t.Run("replicas count once via the advisory queue group", func(t *testing.T) {
		// Two replicas of the same service: identical queue group and the
		// same durable registered on both (both "own" it, as in
		// production where every replica subscribes the consumer).
		newReplica := func() *eventbus.JetStream {
			bus, err := eventbus.ConnectJetStream(context.Background(), eventbus.JetStreamConfig{
				URL: url,
				Streams: []eventbus.StreamSpec{
					{Name: "TEST2", Subjects: []string{"test2.>"}},
				},
				MaxDeliver:    2,
				AckWait:       50 * time.Millisecond,
				DLQQueueGroup: "svc-replica",
			}, logger.New("test"))
			if err != nil {
				t.Fatalf("connect replica: %v", err)
			}
			return bus
		}
		replicaA := newReplica()
		t.Cleanup(func() { _ = replicaA.Close(context.Background()) })
		replicaB := newReplica()
		t.Cleanup(func() { _ = replicaB.Close(context.Background()) })

		for _, r := range []*eventbus.JetStream{replicaA, replicaB} {
			if err := r.Subscribe("test2.poison", "replica-consumer", func(context.Context, eventbus.Message) error {
				return errors.New("always fails")
			}); err != nil {
				t.Fatalf("subscribe replica: %v", err)
			}
		}
		if err := replicaA.Publish(context.Background(), eventbus.Message{
			ID:      "poison-2",
			Subject: "test2.poison",
			Payload: []byte(`{}`),
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}

		// The advisory broadcast hits both replicas; with the queue group
		// exactly one of them delivers it, so the series grows by
		// exactly 1 (2 total with the first subtest's advisory). A
		// broadcast subscription would make both count and reach 3.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if v := scrapeCounterSum(t, handler, "pulsar_eventbus_dlq_advisories_total"); v >= 2 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		// Let any double-count (or a stray second delivery) surface.
		time.Sleep(500 * time.Millisecond)
		if v := scrapeCounterSum(t, handler, "pulsar_eventbus_dlq_advisories_total"); v != 2 {
			t.Errorf("dlq advisories = %v, want 2 (one per scenario: replicas must count once)", v)
		}
	})
}

func scrapeCounter(t *testing.T, h http.Handler, consumer string) (float64, bool) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "pulsar_eventbus_dlq_advisories_total{") && strings.Contains(line, `consumer="`+consumer+`"`) {
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

func scrapeCounterSum(t *testing.T, h http.Handler, name string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var total float64
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err == nil {
			total += v
		}
	}
	return total
}

func scrapeCounterOrZero(t *testing.T, h http.Handler, consumer string) float64 {
	t.Helper()
	v, _ := scrapeCounter(t, h, consumer)
	return v
}
