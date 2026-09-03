package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/internal/core/domain"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
)

// terminalReserveRejection names terminal business rejections that
// must be acknowledged instead of retried: retrying can never change
// the outcome, so the delivery budget is better spent on commands that
// might still succeed (e.g. ones racing their state projection).
//
// sale_not_open is deliberately NOT terminal: commands published
// seconds before the sale opens (clients queued against T-0) die on
// the frontier if acknowledged. As retryable they ride the paced
// redelivery budget — roughly 14s of tolerance with the default
// MaxDeliver — and succeed once the sale opens. The outcome metric
// counts every attempt as sale_not_open, so a sustained rate after
// the window is the signal of a client-side clock or payload problem,
// not a lost command.
func terminalReserveRejection(err error) (reason string, ok bool) {
	if errors.Is(err, domain.ErrSoldOut) {
		return "sold_out", true
	}
	return "", false
}

// Subscribers owns the pulsar-core message handlers: it decodes the
// reservation commands and outcome events and drives the reservation
// use cases. Both the service binary and the e2e suite register the
// same set, so what the tests exercise is exactly what production runs.
type Subscribers struct {
	service *application.ReservationService
	log     *slog.Logger
}

// NewSubscribers wires the core consumers.
func NewSubscribers(service *application.ReservationService, log *slog.Logger) *Subscribers {
	return &Subscribers{service: service, log: log}
}

// Register subscribes every core subject with its production durable
// name. Subscribe failures abort startup: a core that cannot consume is
// worse than a core that never starts.
func (s *Subscribers) Register(bus eventbus.Subscriber) error {
	type consumer struct {
		subject string
		durable string
		handler eventbus.Handler
	}
	for _, c := range []consumer{
		{envelope.SubjectReservationReserve, "core-reserve", s.onReserve},
		{envelope.SubjectPaymentSucceeded, "core-payment-succeeded", s.onPaymentSucceeded},
		{envelope.SubjectPaymentFailed, "core-payment-failed", s.onPaymentFailed},
		{envelope.SubjectReservationExpired, "core-reservation-expired", s.onReservationExpired},
	} {
		if err := bus.Subscribe(c.subject, c.durable, c.handler); err != nil {
			return fmt.Errorf("subscribe %s: %w", c.subject, err)
		}
	}
	return nil
}

// onReserve handles the command carrying the user intent to hold
// tickets.
func (s *Subscribers) onReserve(ctx context.Context, msg eventbus.Message) error {
	var cmd struct {
		ReservationID string `json:"reservation_id"`
		EventID       string `json:"event_id"`
		UserID        string `json:"user_id"`
		Quantity      int    `json:"quantity"`
	}
	if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
		return fmt.Errorf("decode reserve command: %w", err)
	}
	res, err := s.service.Reserve(ctx, application.ReserveCommand{
		ReservationID: cmd.ReservationID,
		EventID:       cmd.EventID,
		UserID:        cmd.UserID,
		Quantity:      cmd.Quantity,
	})
	recordReserveOutcome(err)
	if err != nil {
		// Terminal business outcomes are acknowledged, not retried —
		// see terminalReserveRejection. Everything else stays retryable:
		// sale_not_open (commands queued against the T-0 frontier wait
		// for the window to open under the paced redelivery budget),
		// decode bugs, store errors.
		if reason, terminal := terminalReserveRejection(err); terminal {
			s.log.Info("reservation rejected",
				"event_id", cmd.EventID,
				"reason", reason,
			)
			return nil
		}
		return fmt.Errorf("reserve: %w", err)
	}
	s.log.Info("reservation created",
		"reservation_id", res.ID,
		"event_id", res.EventID,
		"quantity", res.Quantity,
		"expires_at", res.ExpiresAt.Format(time.RFC3339),
	)
	return nil
}

// onPaymentSucceeded handles the payment approval outcome.
func (s *Subscribers) onPaymentSucceeded(ctx context.Context, msg eventbus.Message) error {
	var ev reservationRef
	if err := json.Unmarshal(msg.Payload, &ev); err != nil {
		return fmt.Errorf("decode payment.succeeded: %w", err)
	}
	_, err := s.service.Confirm(ctx, ev.ReservationID)
	recordTerminalOutcome("confirm", err)
	if err != nil {
		return fmt.Errorf("confirm reservation %s: %w", ev.ReservationID, err)
	}
	s.log.Info("reservation confirmed", "reservation_id", ev.ReservationID)
	return nil
}

// onPaymentFailed handles the payment rejection outcome.
func (s *Subscribers) onPaymentFailed(ctx context.Context, msg eventbus.Message) error {
	var ev reservationRef
	if err := json.Unmarshal(msg.Payload, &ev); err != nil {
		return fmt.Errorf("decode payment.failed: %w", err)
	}
	_, err := s.service.Fail(ctx, ev.ReservationID)
	recordTerminalOutcome("fail", err)
	if err != nil {
		return fmt.Errorf("fail reservation %s: %w", ev.ReservationID, err)
	}
	s.log.Info("reservation failed; seat released", "reservation_id", ev.ReservationID)
	return nil
}

// onReservationExpired handles the TTL compensation event.
func (s *Subscribers) onReservationExpired(ctx context.Context, msg eventbus.Message) error {
	var ev reservationRef
	if err := json.Unmarshal(msg.Payload, &ev); err != nil {
		return fmt.Errorf("decode reservation.expired: %w", err)
	}
	_, err := s.service.Expire(ctx, ev.ReservationID)
	recordTerminalOutcome("expire", err)
	if err != nil {
		return fmt.Errorf("expire reservation %s: %w", ev.ReservationID, err)
	}
	s.log.Info("reservation expired; seat released", "reservation_id", ev.ReservationID)
	return nil
}

type reservationRef struct {
	ReservationID string `json:"reservation_id"`
}
