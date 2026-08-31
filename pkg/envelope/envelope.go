// Package envelope defines the single message contract used by every
// PulsarPass event and command published to the broker.
package envelope

import (
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// JetStream streams.
const (
	StreamReservations = "RESERVATIONS"
	StreamPayments     = "PAYMENTS"
)

// Subjects for stream capture patterns.
const (
	SubjectReservationsAll = "pulsarpass.reservations.>"
	SubjectPaymentsAll     = "pulsarpass.payments.>"
)

// Command subjects.
const (
	SubjectReservationReserve = "pulsarpass.reservations.commands.reserve"
	SubjectPaymentProcess     = "pulsarpass.payments.commands.process"
)

// Event subjects.
const (
	SubjectTicketReserved       = "pulsarpass.reservations.events.ticket-reserved"
	SubjectTicketConfirmed      = "pulsarpass.reservations.events.ticket-confirmed"
	SubjectTicketReleased       = "pulsarpass.reservations.events.ticket-released"
	SubjectReservationExpired   = "pulsarpass.reservations.events.reservation-expired"
	SubjectReservationCancelled = "pulsarpass.reservations.events.reservation-cancelled"
	SubjectPaymentSucceeded     = "pulsarpass.payments.events.payment-succeeded"
	SubjectPaymentFailed        = "pulsarpass.payments.events.payment-failed"
)

// Event types carried by Envelope.EventType. Schema evolution happens by
// adding a new version field value, never by breaking existing payloads.
const (
	TypeReservationReserve   = "reservation.reserve"
	TypeTicketReserved       = "ticket.reserved"
	TypeTicketConfirmed      = "ticket.confirmed"
	TypeTicketReleased       = "ticket.released"
	TypeReservationExpired   = "reservation.expired"
	TypeReservationCancelled = "reservation.cancelled"
	TypePaymentProcess       = "payment.process"
	TypePaymentSucceeded     = "payment.succeeded"
	TypePaymentFailed        = "payment.failed"
)

// CurrentVersion is the schema version attached to all new messages.
const CurrentVersion = 1

// Envelope is the transport wrapper around every payload.
type Envelope[T any] struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	EventVersion  int       `json:"event_version"`
	Source        string    `json:"source"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   string    `json:"causation_id,omitempty"`
	OccurredAt    time.Time `json:"occurred_at"`
	Data          T         `json:"data"`
}

// New builds an envelope around data. The EventID doubles as the
// JetStream Nats-Msg-Id for server-side deduplication.
func New[T any](eventType, source, correlationID, causationID string, data T) Envelope[T] {
	return Envelope[T]{
		EventID:       uid.New(),
		EventType:     eventType,
		EventVersion:  CurrentVersion,
		Source:        source,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    time.Now().UTC(),
		Data:          data,
	}
}

// SubjectFor maps an event type to its canonical subject.
func SubjectFor(eventType string) string {
	switch eventType {
	case TypeReservationReserve:
		return SubjectReservationReserve
	case TypeTicketReserved:
		return SubjectTicketReserved
	case TypeTicketConfirmed:
		return SubjectTicketConfirmed
	case TypeTicketReleased:
		return SubjectTicketReleased
	case TypeReservationExpired:
		return SubjectReservationExpired
	case TypeReservationCancelled:
		return SubjectReservationCancelled
	case TypePaymentProcess:
		return SubjectPaymentProcess
	case TypePaymentSucceeded:
		return SubjectPaymentSucceeded
	case TypePaymentFailed:
		return SubjectPaymentFailed
	default:
		return ""
	}
}
