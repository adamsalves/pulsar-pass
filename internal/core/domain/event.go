package domain

import "time"

// Event is the inventory aggregate. ReservedCount and SoldCount are the
// authoritative counters consumed by atomic conditional updates; row
// scans are never used to decide availability.
type Event struct {
	ID            string
	Name          string
	Venue         string
	StartsAt      time.Time
	SaleOpensAt   time.Time
	Capacity      int
	ReservedCount int
	SoldCount     int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Reserve consumes n units of capacity.
func (e *Event) Reserve(n int) error {
	if n <= 0 {
		return ErrInvalidQuantity
	}
	if e.ReservedCount+n+e.SoldCount > e.Capacity {
		return ErrSoldOut
	}
	e.ReservedCount += n
	return nil
}

// Release returns n previously reserved units to the pool.
func (e *Event) Release(n int) error {
	if n <= 0 || n > e.ReservedCount {
		return ErrNotEnoughReserved
	}
	e.ReservedCount -= n
	return nil
}

// ConfirmSold converts n reserved units into sold tickets.
func (e *Event) ConfirmSold(n int) error {
	if n <= 0 || n > e.ReservedCount {
		return ErrNotEnoughReserved
	}
	e.ReservedCount -= n
	e.SoldCount += n
	return nil
}

// SaleIsOpen reports whether sales are open at the given instant.
func (e *Event) SaleIsOpen(now time.Time) bool {
	return !now.Before(e.SaleOpensAt)
}
