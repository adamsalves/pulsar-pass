package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
)

// Subscribers owns the pulsar-payment message handlers: the ticket
// reservation projection into the local reservation context and the
// payment command that runs the charge. Both the service binary and
// the e2e suite register the same set, so what the tests exercise is
// exactly what production runs.
type Subscribers struct {
	processor *Processor
	contexts  ReservationContextRepository
	log       *slog.Logger
}

// NewSubscribers wires the payment consumers.
func NewSubscribers(processor *Processor, contexts ReservationContextRepository, log *slog.Logger) *Subscribers {
	return &Subscribers{processor: processor, contexts: contexts, log: log}
}

// Register subscribes every payment subject with its production durable
// name. Subscribe failures abort startup: a payment service that cannot
// consume is worse than one that never starts.
func (s *Subscribers) Register(bus eventbus.Subscriber) error {
	type consumer struct {
		subject string
		durable string
		handler eventbus.Handler
	}
	for _, c := range []consumer{
		{envelope.SubjectTicketReserved, "payment-context", s.onTicketReserved},
		{envelope.SubjectPaymentProcess, "payment-process", s.onPaymentProcess},
	} {
		if err := bus.Subscribe(c.subject, c.durable, c.handler); err != nil {
			return fmt.Errorf("subscribe %s: %w", c.subject, err)
		}
	}
	return nil
}

// onTicketReserved projects ticket.reserved into the local reservation
// context, which feeds the server-authoritative pricing.
func (s *Subscribers) onTicketReserved(ctx context.Context, msg eventbus.Message) error {
	var ev struct {
		ReservationID string    `json:"reservation_id"`
		UserID        string    `json:"user_id"`
		AmountCents   int64     `json:"amount_cents"`
		Currency      string    `json:"currency"`
		ExpiresAt     time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(msg.Payload, &ev); err != nil {
		return fmt.Errorf("decode ticket.reserved: %w", err)
	}
	if err := s.contexts.Upsert(ctx, ReservationContext{
		ReservationID: ev.ReservationID,
		UserID:        ev.UserID,
		AmountCents:   ev.AmountCents,
		Currency:      ev.Currency,
		ExpiresAt:     ev.ExpiresAt,
	}); err != nil {
		return fmt.Errorf("upsert reservation context: %w", err)
	}
	s.log.Info("reservation context stored", "reservation_id", ev.ReservationID)
	return nil
}

// onPaymentProcess handles the command carrying the user payment
// submission inside the TTL window.
func (s *Subscribers) onPaymentProcess(ctx context.Context, msg eventbus.Message) error {
	var cmd struct {
		ReservationID string `json:"reservation_id"`
		UserID        string `json:"user_id"`
		Token         string `json:"payment_method_token"`
	}
	if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
		return fmt.Errorf("decode payment.process: %w", err)
	}
	idempotencyKey := msg.Headers["Idempotency-Key"]
	if idempotencyKey == "" {
		idempotencyKey = msg.ID
	}
	return s.processor.Handle(ctx, PaymentRequested{
		ReservationID: cmd.ReservationID,
		UserID:        cmd.UserID,
		Token:         cmd.Token,
	}, idempotencyKey)
}
