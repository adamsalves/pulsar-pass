package e2e_test

import (
	"testing"
	"time"
)

// TestSagaSuccessPath drives gateway → core → payment → core end to
// end: the reservation is created, the hold lands in Redis, the payment
// charge is approved and the seat is converted into a sale, with both
// outboxes fully drained.
func TestSagaSuccessPath(t *testing.T) {
	h := boot(t)
	eventID := h.seedEvent(t, 5)

	reservationID, owner := h.createReservation(t, eventID, 2)

	waitFor(t, "reservation PENDING", func() bool {
		return h.reservationStatus(t, reservationID) == "PENDING"
	})
	waitFor(t, "payment context projection", func() bool {
		amount, ok := h.contextAmount(t, reservationID)
		return ok && amount == 2000
	})
	waitFor(t, "redis hold recorded", func() bool { return h.holdExists(reservationID) })

	h.payReservation(t, owner, reservationID, "tok-ok")

	waitFor(t, "reservation CONFIRMED", func() bool {
		return h.reservationStatus(t, reservationID) == "CONFIRMED"
	})
	reserved, sold := h.eventCounts(t, eventID)
	if reserved != 0 || sold != 2 {
		t.Fatalf("event counts = (reserved %d, sold %d), want (0, 2)", reserved, sold)
	}
	if got := h.paymentStatus(t, reservationID); got != "SUCCEEDED" {
		t.Fatalf("payment status = %q, want SUCCEEDED", got)
	}
	waitFor(t, "redis hold released", func() bool { return !h.holdExists(reservationID) })
	waitFor(t, "core outbox drained", func() bool { return h.unprocessedOutbox(t, h.corePool) == 0 })
	waitFor(t, "payment outbox drained", func() bool { return h.unprocessedOutbox(t, h.payPool) == 0 })
}

// TestSagaPaymentDeclined covers the compensation for a rejected
// charge: the reservation fails, the seat returns to the pool and the
// released event is relayed.
func TestSagaPaymentDeclined(t *testing.T) {
	h := boot(t)
	eventID := h.seedEvent(t, 5)

	reservationID, owner := h.createReservation(t, eventID, 1)

	waitFor(t, "reservation PENDING", func() bool {
		return h.reservationStatus(t, reservationID) == "PENDING"
	})
	waitFor(t, "payment context projection", func() bool {
		_, ok := h.contextAmount(t, reservationID)
		return ok
	})

	h.payReservation(t, owner, reservationID, "fail-me")

	waitFor(t, "reservation FAILED", func() bool {
		return h.reservationStatus(t, reservationID) == "FAILED"
	})
	reserved, sold := h.eventCounts(t, eventID)
	if reserved != 0 || sold != 0 {
		t.Fatalf("event counts = (reserved %d, sold %d), want (0, 0)", reserved, sold)
	}
	if got := h.paymentStatus(t, reservationID); got != "FAILED" {
		t.Fatalf("payment status = %q, want FAILED", got)
	}
	waitFor(t, "core outbox drained", func() bool { return h.unprocessedOutbox(t, h.corePool) == 0 })
}

// TestSagaExpirationTTL covers the TTL compensation: an unpaid
// reservation is swept once its natural window elapses, the seat
// returns to the pool, the hold is cleaned up and a late payment
// attempt inside the same saga is rejected by the elapsed window.
func TestSagaExpirationTTL(t *testing.T) {
	h := bootTTL(t, expirationTTL)
	eventID := h.seedEvent(t, 5)

	reservationID, owner := h.createReservation(t, eventID, 1)

	waitFor(t, "reservation PENDING", func() bool {
		return h.reservationStatus(t, reservationID) == "PENDING"
	})
	waitFor(t, "redis hold recorded", func() bool { return h.holdExists(reservationID) })

	waitFor(t, "reservation EXPIRED", func() bool {
		return h.reservationStatus(t, reservationID) == "EXPIRED"
	})
	reserved, sold := h.eventCounts(t, eventID)
	if reserved != 0 || sold != 0 {
		t.Fatalf("event counts = (reserved %d, sold %d), want (0, 0)", reserved, sold)
	}
	waitFor(t, "redis hold cleaned by sweeper", func() bool { return !h.holdExists(reservationID) })
	// A payment submitted after expiry must be rejected by the window
	// check, not charged.
	h.payReservation(t, owner, reservationID, "tok-ok")
	waitFor(t, "late payment rejected", func() bool {
		return h.paymentStatus(t, reservationID) == "FAILED"
	})
	reserved, sold = h.eventCounts(t, eventID)
	if reserved != 0 || sold != 0 {
		t.Fatalf("event counts after late payment = (reserved %d, sold %d), want (0, 0)", reserved, sold)
	}
}

// TestSagaPaymentByNonOwnerRejected covers the ownership check in the
// payment processor: a user who does not own the reservation can
// neither charge against it nor release it. The rejection must be
// side-effect free — the reservation stays PENDING with no payment
// recorded — so the legitimate owner can still complete the purchase.
func TestSagaPaymentByNonOwnerRejected(t *testing.T) {
	h := boot(t)
	eventID := h.seedEvent(t, 5)

	reservationID, owner := h.createReservation(t, eventID, 1)

	waitFor(t, "reservation PENDING", func() bool {
		return h.reservationStatus(t, reservationID) == "PENDING"
	})
	waitFor(t, "payment context projection", func() bool {
		_, ok := h.contextAmount(t, reservationID)
		return ok
	})

	// An impostor submits the payment for someone else's reservation.
	h.payReservation(t, "user-impostor", reservationID, "tok-ok")

	// Give the processor time to consume and reject the command; then
	// verify nothing changed: no payment row, seat still held for the
	// owner and the reservation NOT failed by the impostor attempt.
	time.Sleep(500 * time.Millisecond)
	if status := h.reservationStatus(t, reservationID); status != "PENDING" {
		t.Fatalf("reservation status after impostor attempt = %q, want PENDING", status)
	}
	if got := h.paymentStatus(t, reservationID); got != "" {
		t.Fatalf("payment status after impostor attempt = %q, want none", got)
	}
	if n := h.paymentCount(t, reservationID); n != 0 {
		t.Fatalf("payments recorded after impostor attempt = %d, want 0", n)
	}
	reserved, sold := h.eventCounts(t, eventID)
	if reserved != 1 || sold != 0 {
		t.Fatalf("event counts after impostor attempt = (reserved %d, sold %d), want (1, 0)", reserved, sold)
	}

	// The rightful owner is still able to pay.
	h.payReservation(t, owner, reservationID, "tok-ok")
	waitFor(t, "reservation CONFIRMED by owner", func() bool {
		return h.reservationStatus(t, reservationID) == "CONFIRMED"
	})
	if n := h.paymentCount(t, reservationID); n != 1 {
		t.Fatalf("payments recorded after owner pays = %d, want 1", n)
	}
	if got := h.paymentStatus(t, reservationID); got != "SUCCEEDED" {
		t.Fatalf("payment status after owner pays = %q, want SUCCEEDED", got)
	}
}
