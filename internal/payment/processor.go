package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// ErrNotWired is returned when the processor runs before its adapters
// are configured.
var ErrNotWired = errors.New("payment processor is not wired")

// ErrContextNotFound is returned when the payment command arrives before
// the ticket.reserved projection. It is retryable: the broker redelivers
// until the projection lands.
var ErrContextNotFound = errors.New("reservation context not found")

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

// Processor executes charges and records outcomes in the transactional
// outbox; pulsar-horizon delivers them to the broker.
type Processor struct {
	payments PaymentRepository
	contexts ReservationContextRepository
	outbox   OutboxRepository
	acquirer Acquirer
	clock    Clock
	log      *slog.Logger
	source   string
}

// NewProcessor wires the payment use case.
func NewProcessor(
	payments PaymentRepository,
	contexts ReservationContextRepository,
	outbox OutboxRepository,
	acquirer Acquirer,
	clock Clock,
	log *slog.Logger,
) *Processor {
	return &Processor{
		payments: payments,
		contexts: contexts,
		outbox:   outbox,
		acquirer: acquirer,
		clock:    clock,
		log:      log,
		source:   "pulsar-payment",
	}
}

// Handle processes one payment request end to end.
func (p *Processor) Handle(ctx context.Context, req PaymentRequested, idempotencyKey string) error {
	if p.payments == nil || p.contexts == nil || p.outbox == nil || p.acquirer == nil {
		return ErrNotWired
	}
	if req.ReservationID == "" || idempotencyKey == "" {
		return errors.New("reservation_id and idempotency key are required")
	}

	ctxData, err := p.contexts.Get(ctx, req.ReservationID)
	if err != nil {
		return fmt.Errorf("load reservation context: %w", err)
	}

	userID := req.UserID
	if userID == "" {
		userID = ctxData.UserID
	}
	amount := req.AmountCents
	if amount == 0 {
		amount = ctxData.AmountCents
	}
	currency := req.Currency
	if currency == "" {
		currency = ctxData.Currency
	}
	if currency == "" {
		currency = "BRL"
	}

	pay := &Payment{
		ID:             uid.New(),
		ReservationID:  req.ReservationID,
		UserID:         userID,
		AmountCents:    amount,
		Currency:       currency,
		Status:         PaymentStatusPending,
		IdempotencyKey: idempotencyKey,
	}
	if err := p.payments.Create(ctx, pay); err != nil {
		return err
	}

	if p.clock.Now().After(ctxData.ExpiresAt) {
		return p.finish(ctx, pay, "", "reservation window elapsed", false)
	}

	charge, chargeErr := p.acquirer.Charge(ctx, ChargeRequest{
		ReservationID:  req.ReservationID,
		UserID:         userID,
		AmountCents:    amount,
		Currency:       currency,
		Token:          req.Token,
		IdempotencyKey: idempotencyKey,
	})
	if chargeErr != nil {
		return p.finish(ctx, pay, "", chargeErr.Error(), false)
	}
	return p.finish(ctx, pay, charge.GatewayRef, "", true)
}

func (p *Processor) finish(ctx context.Context, pay *Payment, gatewayRef, reason string, approved bool) error {
	status := PaymentStatusFailed
	eventType := envelope.TypePaymentFailed
	if approved {
		status = PaymentStatusSucceeded
		eventType = envelope.TypePaymentSucceeded
	}
	if err := p.payments.UpdateStatus(ctx, pay.ID, status, gatewayRef, reason); err != nil {
		return err
	}
	rec, err := p.record(eventType, pay, gatewayRef, reason)
	if err != nil {
		return err
	}
	if err := p.outbox.Enqueue(ctx, rec); err != nil {
		return err
	}
	if approved {
		p.log.Info("charge approved",
			"payment_id", pay.ID,
			"reservation_id", pay.ReservationID,
			"gateway_ref", gatewayRef,
		)
		return nil
	}
	p.log.Warn("charge rejected",
		"payment_id", pay.ID,
		"reservation_id", pay.ReservationID,
		"reason", reason,
	)
	return nil
}

func (p *Processor) record(eventType string, pay *Payment, gatewayRef, reason string) (OutboxRecord, error) {
	payload, err := json.Marshal(PaymentOutcomePayload{
		ReservationID: pay.ReservationID,
		PaymentID:     pay.ID,
		AmountCents:   pay.AmountCents,
		GatewayRef:    gatewayRef,
		Reason:        reason,
	})
	if err != nil {
		return OutboxRecord{}, err
	}
	env := envelope.New(eventType, p.source, pay.ReservationID, "", json.RawMessage(payload))
	return OutboxRecord{
		ID:            env.EventID,
		Subject:       envelope.SubjectFor(eventType),
		EventType:     env.EventType,
		EventVersion:  env.EventVersion,
		Source:        env.Source,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Payload:       payload,
		OccurredAt:    env.OccurredAt,
	}, nil
}
