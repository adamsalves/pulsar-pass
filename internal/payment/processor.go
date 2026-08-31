package payment

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// ErrNotWired is returned when the processor runs before its adapters
// are configured.
var ErrNotWired = errors.New("payment processor is not wired")

// PaymentRequested is the command consumed from the broker.
type PaymentRequested struct {
	ReservationID string `json:"reservation_id"`
	UserID        string `json:"user_id"`
	AmountCents   int64  `json:"amount_cents"`
	Currency      string `json:"currency"`
	Token         string `json:"payment_method_token"`
}

// PaymentOutcomePayload is published after the charge attempt.
type PaymentOutcomePayload struct {
	ReservationID string `json:"reservation_id"`
	PaymentID     string `json:"payment_id"`
	AmountCents   int64  `json:"amount_cents"`
	GatewayRef    string `json:"gateway_ref,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

// Processor executes charges and reports outcomes to the broker.
type Processor struct {
	repo     PaymentRepository
	acquirer Acquirer
	bus      eventbus.Publisher
	log      *slog.Logger
	source   string
}

// NewProcessor wires the payment use case.
func NewProcessor(repo PaymentRepository, acquirer Acquirer, bus eventbus.Publisher, log *slog.Logger) *Processor {
	return &Processor{
		repo:     repo,
		acquirer: acquirer,
		bus:      bus,
		log:      log,
		source:   "pulsar-payment",
	}
}

// Handle processes one payment request end to end. Outcome publication
// moves to the transactional outbox when the payment database adapter
// lands.
func (p *Processor) Handle(ctx context.Context, req PaymentRequested, idempotencyKey string) error {
	if p.repo == nil || p.acquirer == nil || p.bus == nil {
		return ErrNotWired
	}
	if req.ReservationID == "" || idempotencyKey == "" {
		return errors.New("reservation_id and idempotency key are required")
	}

	pay := &Payment{
		ID:             uid.New(),
		ReservationID:  req.ReservationID,
		UserID:         req.UserID,
		AmountCents:    req.AmountCents,
		Currency:       req.Currency,
		Status:         PaymentStatusPending,
		IdempotencyKey: idempotencyKey,
	}
	if pay.Currency == "" {
		pay.Currency = "BRL"
	}
	if err := p.repo.Create(ctx, pay); err != nil {
		return err
	}

	charge, chargeErr := p.acquirer.Charge(ctx, ChargeRequest{
		ReservationID:  req.ReservationID,
		UserID:         req.UserID,
		AmountCents:    req.AmountCents,
		Currency:       pay.Currency,
		Token:          req.Token,
		IdempotencyKey: idempotencyKey,
	})
	if chargeErr != nil {
		if err := p.repo.UpdateStatus(ctx, pay.ID, PaymentStatusFailed, "", chargeErr.Error()); err != nil {
			return err
		}
		if err := p.publishOutcome(ctx, pay, envelope.SubjectPaymentFailed, "", chargeErr.Error()); err != nil {
			return err
		}
		p.log.Warn("charge rejected",
			"payment_id", pay.ID,
			"reservation_id", pay.ReservationID,
			"reason", chargeErr.Error(),
		)
		return nil
	}

	if err := p.repo.UpdateStatus(ctx, pay.ID, PaymentStatusSucceeded, charge.GatewayRef, ""); err != nil {
		return err
	}
	if err := p.publishOutcome(ctx, pay, envelope.SubjectPaymentSucceeded, charge.GatewayRef, ""); err != nil {
		return err
	}
	p.log.Info("charge approved",
		"payment_id", pay.ID,
		"reservation_id", pay.ReservationID,
		"gateway_ref", charge.GatewayRef,
	)
	return nil
}

func (p *Processor) publishOutcome(ctx context.Context, pay *Payment, subject, gatewayRef, reason string) error {
	payload, err := json.Marshal(PaymentOutcomePayload{
		ReservationID: pay.ReservationID,
		PaymentID:     pay.ID,
		AmountCents:   pay.AmountCents,
		GatewayRef:    gatewayRef,
		Reason:        reason,
	})
	if err != nil {
		return err
	}
	msg := eventbus.Message{
		// Stable id per outcome: a redelivered command re-executed after
		// the same payment record dedups on the broker.
		ID:      pay.ID + "-" + outcomeSuffix(subject),
		Subject: subject,
		Payload: payload,
		Headers: map[string]string{
			"Correlation-Id": pay.ReservationID,
		},
	}
	return p.bus.Publish(ctx, msg)
}

func outcomeSuffix(subject string) string {
	if subject == envelope.SubjectPaymentSucceeded {
		return "succeeded"
	}
	return "failed"
}
