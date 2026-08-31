package domain

import (
	"fmt"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// ReservationStatus is the state machine of a reservation. Only the
// transitions implemented on Reservation are legal; anything else must
// fail loudly.
type ReservationStatus string

const (
	ReservationStatusPending   ReservationStatus = "PENDING"
	ReservationStatusConfirmed ReservationStatus = "CONFIRMED"
	ReservationStatusExpired   ReservationStatus = "EXPIRED"
	ReservationStatusFailed    ReservationStatus = "FAILED"
	ReservationStatusCancelled ReservationStatus = "CANCELLED"
)

// Reservation is the aggregate root of the booking flow.
type Reservation struct {
	ID          string
	EventID     string
	UserID      string
	Status      ReservationStatus
	Quantity    int
	AmountCents int64
	Currency    string
	ExpiresAt   time.Time
	ConfirmedAt *time.Time
	Version     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewReservationInput carries the data needed to start a reservation.
type NewReservationInput struct {
	EventID     string
	UserID      string
	Quantity    int
	AmountCents int64
	Currency    string
	TTL         time.Duration
	Now         time.Time
}

// NewReservation creates a reservation in its initial PENDING state with
// the retention window applied from Now.
func NewReservation(in NewReservationInput) *Reservation {
	currency := in.Currency
	if currency == "" {
		currency = "BRL"
	}
	return &Reservation{
		ID:          uid.New(),
		EventID:     in.EventID,
		UserID:      in.UserID,
		Status:      ReservationStatusPending,
		Quantity:    in.Quantity,
		AmountCents: in.AmountCents,
		Currency:    currency,
		ExpiresAt:   in.Now.Add(in.TTL),
		Version:     1,
		CreatedAt:   in.Now,
		UpdatedAt:   in.Now,
	}
}

// Confirm finalizes a pending reservation. It refuses late confirmations
// past the retention window.
func (r *Reservation) Confirm(now time.Time) error {
	if r.Status != ReservationStatusPending {
		return invalidTransition(r.Status, ReservationStatusConfirmed)
	}
	if now.After(r.ExpiresAt) {
		return ErrReservationExpired
	}
	confirmed := now
	r.Status = ReservationStatusConfirmed
	r.ConfirmedAt = &confirmed
	r.touch(now)
	return nil
}

// Expire moves a pending reservation whose window elapsed to EXPIRED.
func (r *Reservation) Expire(now time.Time) error {
	if r.Status != ReservationStatusPending {
		return invalidTransition(r.Status, ReservationStatusExpired)
	}
	if !now.After(r.ExpiresAt) {
		return ErrReservationNotExpired
	}
	r.Status = ReservationStatusExpired
	r.touch(now)
	return nil
}

// MarkFailed moves a pending reservation rejected by payment to FAILED.
func (r *Reservation) MarkFailed(now time.Time) error {
	if r.Status != ReservationStatusPending {
		return invalidTransition(r.Status, ReservationStatusFailed)
	}
	r.Status = ReservationStatusFailed
	r.touch(now)
	return nil
}

// Cancel moves a pending reservation cancelled by the user to CANCELLED.
func (r *Reservation) Cancel(now time.Time) error {
	if r.Status != ReservationStatusPending {
		return invalidTransition(r.Status, ReservationStatusCancelled)
	}
	r.Status = ReservationStatusCancelled
	r.touch(now)
	return nil
}

func (r *Reservation) touch(now time.Time) {
	r.Version++
	r.UpdatedAt = now
}

func invalidTransition(from, to ReservationStatus) error {
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
}
