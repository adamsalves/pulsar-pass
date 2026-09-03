// Package broker assembles the JetStream topology shared by all
// PulsarPass services.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
)

// Streams returns the shared stream topology derived from the event
// contract constants.
func Streams() []eventbus.StreamSpec {
	return []eventbus.StreamSpec{
		{Name: envelope.StreamReservations, Subjects: []string{envelope.SubjectReservationsAll}},
		{Name: envelope.StreamPayments, Subjects: []string{envelope.SubjectPaymentsAll}},
	}
}

// Connect opens a JetStream-backed bus with the shared topology and
// production defaults. Handler failures are paced by the eventbus
// (NakWithDelay growing with the delivery attempt, budget bounded by
// MaxDeliver), so a command racing its state — payment.process arriving
// before the reservation context projection — stays retryable for
// seconds instead of burning its delivery budget instantly. name is the
// service identity: replicas of the same service join one DLQ advisory
// queue group so the DLQ metric counts once per service, not once per
// replica.
func Connect(ctx context.Context, url, name string, log *slog.Logger) (*eventbus.JetStream, error) {
	bus, err := eventbus.ConnectJetStream(ctx, eventbus.JetStreamConfig{
		URL:           url,
		Streams:       Streams(),
		DedupWindow:   2 * time.Minute,
		DLQQueueGroup: name,
	}, log)
	if err != nil {
		return nil, fmt.Errorf("broker connect: %w", err)
	}
	return bus, nil
}
