package domain

import (
	"fmt"
	"time"
)

// TicketStatus is the lifecycle of an individual seat.
type TicketStatus string

const (
	TicketStatusAvailable TicketStatus = "AVAILABLE"
	TicketStatusReserved  TicketStatus = "RESERVED"
	TicketStatusSold      TicketStatus = "SOLD"
)

// Ticket is a single seat attached to an event and, while held, to a
// reservation.
type Ticket struct {
	ID            string
	EventID       string
	ReservationID string
	SeatLabel     string
	Status        TicketStatus
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// MarkReserved holds an available seat for a reservation.
func (t *Ticket) MarkReserved(reservationID string, now time.Time) error {
	if t.Status != TicketStatusAvailable {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, TicketStatusReserved)
	}
	t.Status = TicketStatusReserved
	t.ReservationID = reservationID
	t.UpdatedAt = now
	return nil
}

// MarkSold finalizes a held seat.
func (t *Ticket) MarkSold(now time.Time) error {
	if t.Status != TicketStatusReserved {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, TicketStatusSold)
	}
	t.Status = TicketStatusSold
	t.UpdatedAt = now
	return nil
}

// Release returns a held seat to the available pool.
func (t *Ticket) Release(now time.Time) error {
	if t.Status != TicketStatusReserved {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, TicketStatusAvailable)
	}
	t.Status = TicketStatusAvailable
	t.ReservationID = ""
	t.UpdatedAt = now
	return nil
}
