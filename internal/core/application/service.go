package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core/domain"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// ReservationService orchestrates the reservation state machine. All
// emitted events go through the transactional outbox; nothing is
// published directly from this service.
type ReservationService struct {
	reservations ReservationRepository
	inventory    InventoryRepository
	outbox       OutboxRepository
	tx           TxRunner
	clock        Clock
	ttl          time.Duration
	source       string
}

// NewReservationService wires the reservation use cases. tx is optional:
// when nil each port call runs on its own (tests, tools); when set, the
// whole use case commits as a single unit of work.
func NewReservationService(
	reservations ReservationRepository,
	inventory InventoryRepository,
	outbox OutboxRepository,
	tx TxRunner,
	clock Clock,
	ttl time.Duration,
) *ReservationService {
	return &ReservationService{
		reservations: reservations,
		inventory:    inventory,
		outbox:       outbox,
		tx:           tx,
		clock:        clock,
		ttl:          ttl,
		source:       "pulsar-core",
	}
}

// ReserveCommand carries the user intent to hold tickets.
type ReserveCommand struct {
	// ReservationID is the client-provided reservation identifier
	// returned to the user by the gateway.
	ReservationID string
	EventID       string
	UserID        string
	Quantity      int
}

// Reserve consumes capacity, creates the pending reservation and
// enqueues ticket.reserved. Repository adapters guarantee that capacity,
// reservation and outbox rows commit in a single transaction.
func (s *ReservationService) Reserve(ctx context.Context, cmd ReserveCommand) (*domain.Reservation, error) {
	res, err := s.inTx(ctx, func(txctx context.Context) (*domain.Reservation, error) {
		return s.reserve(txctx, cmd)
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Confirm finalizes a pending reservation after payment success.
func (s *ReservationService) Confirm(ctx context.Context, reservationID string) (*domain.Reservation, error) {
	res, err := s.inTx(ctx, func(txctx context.Context) (*domain.Reservation, error) {
		return s.confirm(txctx, reservationID)
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Expire releases a pending reservation whose TTL elapsed.
func (s *ReservationService) Expire(ctx context.Context, reservationID string) (*domain.Reservation, error) {
	return s.release(ctx, reservationID, reasonExpired)
}

// Fail releases a pending reservation rejected by payment.
func (s *ReservationService) Fail(ctx context.Context, reservationID string) (*domain.Reservation, error) {
	return s.release(ctx, reservationID, reasonFailed)
}

// Cancel releases a pending reservation cancelled by the user.
func (s *ReservationService) Cancel(ctx context.Context, reservationID string) (*domain.Reservation, error) {
	return s.release(ctx, reservationID, reasonCancelled)
}

type releaseReason string

const (
	reasonExpired   releaseReason = "expired"
	reasonFailed    releaseReason = "payment_failed"
	reasonCancelled releaseReason = "cancelled"
)

func (s *ReservationService) reserve(ctx context.Context, cmd ReserveCommand) (*domain.Reservation, error) {
	if cmd.EventID == "" || cmd.UserID == "" || cmd.Quantity <= 0 {
		return nil, domain.ErrInvalidQuantity
	}
	if cmd.ReservationID != "" && !uid.IsValid(cmd.ReservationID) {
		return nil, domain.ErrInvalidID
	}
	// Idempotent replay guard: the gateway generates the reservation id,
	// so a Create conflict on redelivery means the command was already
	// applied. Returning the existing reservation avoids relying on the
	// transaction rollback and keeps the consumer quiet.
	if cmd.ReservationID != "" {
		if existing, err := s.reservations.Get(ctx, cmd.ReservationID); err == nil {
			return existing, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return nil, err
		}
	}
	now := s.clock.Now()
	event, err := s.inventory.Event(ctx, cmd.EventID)
	if err != nil {
		return nil, err
	}
	if !event.SaleIsOpen(now) {
		return nil, domain.ErrSaleNotOpen
	}
	if err := s.inventory.ReserveCapacity(ctx, cmd.EventID, cmd.Quantity); err != nil {
		return nil, err
	}
	res := domain.NewReservation(domain.NewReservationInput{
		ID:          cmd.ReservationID,
		EventID:     cmd.EventID,
		UserID:      cmd.UserID,
		Quantity:    cmd.Quantity,
		AmountCents: event.PriceCents * int64(cmd.Quantity),
		TTL:         s.ttl,
		Now:         now,
	})
	if err := s.reservations.Create(ctx, res); err != nil {
		return nil, err
	}
	rec, err := s.record(envelope.TypeTicketReserved, res.ID, "", TicketReservedPayload{
		ReservationID: res.ID,
		EventID:       res.EventID,
		UserID:        res.UserID,
		Quantity:      res.Quantity,
		AmountCents:   res.AmountCents,
		Currency:      res.Currency,
		ExpiresAt:     res.ExpiresAt,
	})
	if err != nil {
		return nil, err
	}
	if err := s.outbox.Enqueue(ctx, rec); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *ReservationService) confirm(ctx context.Context, reservationID string) (*domain.Reservation, error) {
	res, err := s.load(ctx, reservationID)
	if err != nil {
		return nil, err
	}
	// Idempotent replay: the outcome event was already applied in a
	// previous delivery, so skip side effects entirely.
	if res.Status == domain.ReservationStatusConfirmed {
		return res, nil
	}
	if res.Status != domain.ReservationStatusPending {
		return nil, fmt.Errorf("%w: cannot confirm reservation in status %s", domain.ErrInvalidTransition, res.Status)
	}
	// No expiry gate here: the payment service only charges inside the
	// retention window, so a succeeded payment is honored even if relay
	// lag delivers the event past ExpiresAt.
	now := s.clock.Now()
	if err := res.Confirm(now); err != nil {
		return nil, err
	}
	if err := s.inventory.CommitSold(ctx, res.EventID, res.Quantity); err != nil {
		return nil, err
	}
	if err := s.reservations.Update(ctx, res); err != nil {
		return nil, err
	}
	rec, err := s.record(envelope.TypeTicketConfirmed, res.ID, "", TicketConfirmedPayload{
		ReservationID: res.ID,
		EventID:       res.EventID,
		Quantity:      res.Quantity,
	})
	if err != nil {
		return nil, err
	}
	return res, s.outbox.Enqueue(ctx, rec)
}

func (s *ReservationService) release(ctx context.Context, reservationID string, reason releaseReason) (*domain.Reservation, error) {
	res, err := s.inTx(ctx, func(txctx context.Context) (*domain.Reservation, error) {
		return s.releaseInTx(txctx, reservationID, reason)
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *ReservationService) releaseInTx(ctx context.Context, reservationID string, reason releaseReason) (*domain.Reservation, error) {
	res, err := s.load(ctx, reservationID)
	if err != nil {
		return nil, err
	}
	// Idempotent replay: any terminal (released) state means the seat is
	// already back in the pool, so skip side effects entirely. This also
	// covers cross-cause replays, e.g. payment.failed arriving after the
	// sweeper already expired the reservation.
	switch res.Status {
	case domain.ReservationStatusExpired,
		domain.ReservationStatusFailed,
		domain.ReservationStatusCancelled:
		return res, nil
	}
	now := s.clock.Now()
	switch reason {
	case reasonExpired:
		err = res.Expire(now)
	case reasonFailed:
		err = res.MarkFailed(now)
	default:
		err = res.Cancel(now)
	}
	if err != nil {
		return nil, err
	}
	if err := s.inventory.ReleaseCapacity(ctx, res.EventID, res.Quantity); err != nil {
		return nil, err
	}
	if err := s.reservations.Update(ctx, res); err != nil {
		return nil, err
	}
	rec, err := s.record(envelope.TypeTicketReleased, res.ID, "", TicketReleasedPayload{
		ReservationID: res.ID,
		EventID:       res.EventID,
		Quantity:      res.Quantity,
		Reason:        string(reason),
	})
	if err != nil {
		return nil, err
	}
	return res, s.outbox.Enqueue(ctx, rec)
}

func (s *ReservationService) inTx(ctx context.Context, fn func(ctx context.Context) (*domain.Reservation, error)) (*domain.Reservation, error) {
	if s.tx == nil {
		return fn(ctx)
	}
	var out *domain.Reservation
	err := s.tx.WithinTx(ctx, func(txctx context.Context) error {
		var e error
		out, e = fn(txctx)
		return e
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ReservationService) load(ctx context.Context, id string) (*domain.Reservation, error) {
	if id == "" {
		return nil, domain.ErrNotFound
	}
	return s.reservations.Get(ctx, id)
}

func (s *ReservationService) record(eventType, correlationID, causationID string, payload any) (OutboxRecord, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return OutboxRecord{}, err
	}
	env := envelope.New(eventType, s.source, correlationID, causationID, json.RawMessage(data))
	return OutboxRecord{
		ID:            env.EventID,
		Subject:       envelope.SubjectFor(eventType),
		EventType:     env.EventType,
		EventVersion:  env.EventVersion,
		Source:        env.Source,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Payload:       data,
		OccurredAt:    env.OccurredAt,
	}, nil
}
