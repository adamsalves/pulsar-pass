package domain

import "errors"

var (
	ErrInvalidTransition     = errors.New("invalid state transition")
	ErrSoldOut               = errors.New("event sold out")
	ErrReservationExpired    = errors.New("reservation expired")
	ErrReservationNotExpired = errors.New("reservation not expired yet")
	ErrInvalidQuantity       = errors.New("quantity must be positive")
	ErrNotEnoughReserved     = errors.New("not enough reserved units")
	ErrSaleNotOpen           = errors.New("sale is not open")
	ErrNotFound              = errors.New("resource not found")
)
