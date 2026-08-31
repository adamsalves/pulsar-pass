package eventbus_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
)

func startEmbeddedNATS(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		JetStream: true,
		Port:      -1,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create embedded nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

func newTestBus(t *testing.T, tune func(*eventbus.JetStreamConfig)) *eventbus.JetStream {
	t.Helper()
	cfg := eventbus.JetStreamConfig{
		URL: startEmbeddedNATS(t),
		Streams: []eventbus.StreamSpec{
			{Name: "TEST", Subjects: []string{"test.>"}},
		},
	}
	if tune != nil {
		tune(&cfg)
	}
	bus, err := eventbus.ConnectJetStream(context.Background(), cfg, logger.New("test"))
	if err != nil {
		t.Fatalf("connect jetstream: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	return bus
}

func TestJetStreamPublishSubscribeRoundtrip(t *testing.T) {
	bus := newTestBus(t, nil)

	received := make(chan eventbus.Message, 8)
	if err := bus.Subscribe("test.a", "test-consumer", func(_ context.Context, msg eventbus.Message) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	for _, id := range []string{"m1", "m2", "m3"} {
		if err := bus.Publish(context.Background(), eventbus.Message{
			ID:      id,
			Subject: "test.a",
			Payload: []byte(id),
		}); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}

	seen := make(map[string]bool, 3)
	for range 3 {
		select {
		case msg := <-received:
			if seen[string(msg.Payload)] {
				t.Fatalf("unexpected duplicate delivery of %s", msg.Payload)
			}
			seen[string(msg.Payload)] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout; received %d of 3 messages", len(seen))
		}
	}
}

func TestJetStreamDeduplicatesByMessageID(t *testing.T) {
	bus := newTestBus(t, nil)

	received := make(chan eventbus.Message, 8)
	if err := bus.Subscribe("test.dedup", "test-dedup", func(_ context.Context, msg eventbus.Message) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	for range 3 {
		if err := bus.Publish(context.Background(), eventbus.Message{
			ID:      "same-id",
			Subject: "test.dedup",
			Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the single delivery")
	}
	select {
	case extra := <-received:
		t.Fatalf("dedup failed; got extra delivery %q", extra.Payload)
	case <-time.After(1500 * time.Millisecond):
	}
}

func TestJetStreamRedeliveryOnHandlerFailure(t *testing.T) {
	bus := newTestBus(t, func(cfg *eventbus.JetStreamConfig) {
		cfg.AckWait = time.Second
		cfg.MaxDeliver = 3
		cfg.BackOff = []time.Duration{200 * time.Millisecond}
	})

	deliveries := make(chan eventbus.Message, 8)
	// atomic because the handler runs on the consumer goroutine while
	// the test goroutine flips it after the first delivery.
	var fail atomic.Bool
	fail.Store(true)
	if err := bus.Subscribe("test.retry", "test-retry", func(_ context.Context, msg eventbus.Message) error {
		deliveries <- msg
		if fail.Load() {
			return errors.New("boom")
		}
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), eventbus.Message{
		ID:      "r1",
		Subject: "test.retry",
		Payload: []byte("r1"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-deliveries:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}
	fail.Store(false)
	select {
	case <-deliveries:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for redelivery after handler failure")
	}
}
