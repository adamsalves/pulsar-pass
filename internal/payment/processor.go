package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// ErrNotWired is returned when the processor runs before its adapters
// are configured.
var ErrNotWired = errors.New("payment processor is not wired")

// ErrContextNotFound is returned when the payment command arrives before
// the ticket.reserved projection. It is retryable: the processor waits
// for the projection inline for a bounded budget and, if it still does
// not land, returns the error so the broker redelivers with pacing.
var ErrContextNotFound = errors.New("reservation context not found")

// ErrDuplicatePayment is returned by the repository when a payment with
// the same idempotency key already exists — the signature of a
// redelivered command.
var ErrDuplicatePayment = errors.New("payment idempotency key already registered")

// ErrPaymentNotFound is returned when a lookup by idempotency key finds
// no row.
var ErrPaymentNotFound = errors.New("payment not found")

// ErrNotOwner is returned when the payment command's user does not
// match the reservation owner projected from ticket.reserved. The
// rejection is terminal and side-effect free: no payment is recorded,
// no outcome is published and the acquirer is never called, so an
// impostor can neither charge against someone else's reservation nor
// release it. The redelivered command eventually reaches the DLQ.
var ErrNotOwner = errors.New("payment submitted by non-owner")

// PaymentRequested is the command consumed from the broker. It carries
// no monetary fields: amount and currency come exclusively from the
// reservation context projection, keeping pricing server-authoritative.
// UserID is the payer identity; Handle rejects commands whose user does
// not own the projected reservation.
type PaymentRequested struct {
	ReservationID string `json:"reservation_id"`
	UserID        string `json:"user_id"`
	Token         string `json:"payment_method_token"`
}

// Inline wait budget for the reservation context: a payment command can
// legitimately race ticket.reserved through the outbox relay (the user
// paying immediately after reserving). Instead of burning the broker's
// redelivery budget on a race that resolves in milliseconds, the
// processor re-reads the projection inline before handing the retryable
// error back for paced redelivery.
const (
	defaultContextWaitAttempts = 3
	defaultContextWaitDelay    = 500 * time.Millisecond
)

// Option customizes the processor (tests, tooling). The zero value
// keeps production defaults.
type Option func(*Processor)

// WithSleeper replaces the wait between context re-reads. The default
// honors context cancellation (shutdown aborts the wait); tests inject
// an immediate return to stay deterministic under the race detector.
func WithSleeper(sleep func(ctx context.Context, d time.Duration) error) Option {
	return func(p *Processor) {
		if sleep != nil {
			p.sleeper = sleep
		}
	}
}

// WithContextWait overrides the inline wait budget (attempts × delay).
// Non-positive values keep the defaults.
func WithContextWait(attempts int, delay time.Duration) Option {
	return func(p *Processor) {
		if attempts > 0 {
			p.contextWaitAttempts = attempts
		}
		if delay > 0 {
			p.contextWaitDelay = delay
		}
	}
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
// outbox; pulsar-horizon delivers them to the broker. Outcome writes
// (status + outbox) commit atomically, and redelivered commands resume
// from the stored payment instead of charging again.
type Processor struct {
	payments            PaymentRepository
	contexts            ReservationContextRepository
	outbox              OutboxRepository
	tx                  TxRunner
	acquirer            Acquirer
	clock               Clock
	log                 *slog.Logger
	source              string
	sleeper             func(ctx context.Context, d time.Duration) error
	contextWaitAttempts int
	contextWaitDelay    time.Duration
}

// NewProcessor wires the payment use case. tx is optional: when nil,
// outcome writes run as separate statements (tests, tools).
func NewProcessor(
	payments PaymentRepository,
	contexts ReservationContextRepository,
	outbox OutboxRepository,
	tx TxRunner,
	acquirer Acquirer,
	clock Clock,
	log *slog.Logger,
	opts ...Option,
) *Processor {
	p := &Processor{
		payments:            payments,
		contexts:            contexts,
		outbox:              outbox,
		tx:                  tx,
		acquirer:            acquirer,
		clock:               clock,
		log:                 log,
		source:              "pulsar-payment",
		sleeper:             sleepFor,
		contextWaitAttempts: defaultContextWaitAttempts,
		contextWaitDelay:    defaultContextWaitDelay,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// sleepFor waits d, aborting as soon as ctx is done — a shutdown must
// not park a consumer for the remainder of the wait budget.
func sleepFor(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Handle processes one payment request end to end.
func (p *Processor) Handle(ctx context.Context, req PaymentRequested, idempotencyKey string) error {
	if p.payments == nil || p.contexts == nil || p.outbox == nil || p.acquirer == nil {
		return ErrNotWired
	}
	if req.ReservationID == "" || req.UserID == "" || idempotencyKey == "" {
		return errors.New("reservation_id, user_id and idempotency key are required")
	}

	ctxData, err := p.waitContext(ctx, req.ReservationID)
	if err != nil {
		return fmt.Errorf("load reservation context: %w", err)
	}

	if req.UserID != ctxData.UserID {
		// The projection is authoritative: only the user who created the
		// reservation may pay it. Reject before any write so the state of
		// the reservation stays untouched for the legitimate owner.
		p.log.Warn("payment rejected: submitter is not the reservation owner",
			"reservation_id", req.ReservationID,
			"submitted_by", req.UserID,
		)
		return fmt.Errorf("%w: user %q does not own reservation", ErrNotOwner, req.UserID)
	}

	pay, err := p.loadOrCreate(ctx, req, ctxData, idempotencyKey)
	if err != nil {
		return err
	}
	if pay.Status != PaymentStatusPending {
		// A previous run already decided this payment but the outcome
		// event may not have been delivered (crash between status and
		// outbox writes). Re-record it from the stored state; the charge
		// itself is never retried for decided payments.
		p.log.Info("payment already decided; re-recording outcome",
			"payment_id", pay.ID,
			"reservation_id", pay.ReservationID,
			"status", pay.Status,
		)
		return p.recordOutcome(ctx, pay)
	}

	if p.clock.Now().After(ctxData.ExpiresAt) {
		return p.finish(ctx, pay, "", "reservation window elapsed", false)
	}

	charge, chargeErr := p.acquirer.Charge(ctx, ChargeRequest{
		ReservationID:  req.ReservationID,
		UserID:         pay.UserID,
		AmountCents:    pay.AmountCents,
		Currency:       pay.Currency,
		Token:          req.Token,
		IdempotencyKey: idempotencyKey,
	})
	if chargeErr != nil {
		return p.finish(ctx, pay, "", chargeErr.Error(), false)
	}
	return p.finish(ctx, pay, charge.GatewayRef, "", true)
}

// waitContext reads the reservation context, tolerating the race with
// the ticket.reserved projection: when the command beats the outbox
// relay, the projection is re-read up to contextWaitAttempts times
// inline before the retryable sentinel goes back to the broker for
// paced redelivery. Cancellation (shutdown) aborts the wait and keeps
// the retryable semantics — the command is simply redelivered later.
func (p *Processor) waitContext(ctx context.Context, reservationID string) (ReservationContext, error) {
	ctxData, err := p.contexts.Get(ctx, reservationID)
	if !errors.Is(err, ErrContextNotFound) {
		return ctxData, err
	}
	p.log.Info("payment raced the reservation projection; waiting inline",
		"reservation_id", reservationID,
	)
	for attempt := 0; attempt < p.contextWaitAttempts; attempt++ {
		if sleepErr := p.sleeper(ctx, p.contextWaitDelay); sleepErr != nil {
			return ReservationContext{}, err
		}
		ctxData, retryErr := p.contexts.Get(ctx, reservationID)
		if errors.Is(retryErr, ErrContextNotFound) {
			continue
		}
		if retryErr != nil {
			return ReservationContext{}, retryErr
		}
		recordContextWait("resolved")
		return ctxData, nil
	}
	recordContextWait("exhausted")
	p.log.Warn("reservation context did not land within the inline wait; returning to broker",
		"reservation_id", reservationID,
		"attempts", p.contextWaitAttempts,
	)
	return ReservationContext{}, err
}

// loadOrCreate persists a fresh payment attempt or, for redelivered
// commands, returns the existing one keyed by idempotency key.
func (p *Processor) loadOrCreate(ctx context.Context, req PaymentRequested, ctxData ReservationContext, idempotencyKey string) (*Payment, error) {
	pay := &Payment{
		ID:             uid.New(),
		ReservationID:  req.ReservationID,
		UserID:         ctxData.UserID,
		AmountCents:    ctxData.AmountCents,
		Currency:       ctxData.Currency,
		Status:         PaymentStatusPending,
		IdempotencyKey: idempotencyKey,
	}
	if pay.Currency == "" {
		pay.Currency = "BRL"
	}
	err := p.payments.Create(ctx, pay)
	if err == nil {
		return pay, nil
	}
	if !errors.Is(err, ErrDuplicatePayment) {
		return nil, err
	}
	existing, err := p.payments.GetByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("load existing payment by idempotency key: %w", err)
	}
	return existing, nil
}

// finish persists the decision and its outcome event atomically.
func (p *Processor) finish(ctx context.Context, pay *Payment, gatewayRef, reason string, approved bool) error {
	status := PaymentStatusFailed
	eventType := envelope.TypePaymentFailed
	if approved {
		status = PaymentStatusSucceeded
		eventType = envelope.TypePaymentSucceeded
	}
	recordChargeOutcome(approved, reason)
	rec, err := p.record(eventType, pay, gatewayRef, reason)
	if err != nil {
		return err
	}
	write := func(txctx context.Context) error {
		if err := p.payments.UpdateStatus(txctx, pay.ID, status, gatewayRef, reason); err != nil {
			return err
		}
		return p.outbox.Enqueue(txctx, rec)
	}
	if p.tx == nil {
		err = write(ctx)
	} else {
		err = p.tx.WithinTx(ctx, write)
	}
	if err != nil {
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

// recordOutcome re-enqueues the outcome event for an already decided
// payment. A fresh event id may duplicate a previously delivered
// outcome; downstream consumers are idempotent by design.
func (p *Processor) recordOutcome(ctx context.Context, pay *Payment) error {
	var eventType, gatewayRef, reason string
	switch pay.Status {
	case PaymentStatusSucceeded:
		eventType, gatewayRef = envelope.TypePaymentSucceeded, pay.GatewayRef
	case PaymentStatusFailed:
		eventType, reason = envelope.TypePaymentFailed, pay.FailureReason
	default:
		return fmt.Errorf("cannot record outcome for payment in status %q", pay.Status)
	}
	rec, err := p.record(eventType, pay, gatewayRef, reason)
	if err != nil {
		return err
	}
	return p.outbox.Enqueue(ctx, rec)
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
