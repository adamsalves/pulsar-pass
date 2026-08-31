package application

import "time"

// TicketReservedPayload is emitted when a seat is held successfully.
type TicketReservedPayload struct {
	ReservationID string    `json:"reservation_id"`
	EventID       string    `json:"event_id"`
	UserID        string    `json:"user_id"`
	Quantity      int       `json:"quantity"`
	AmountCents   int64     `json:"amount_cents"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// TicketConfirmedPayload is emitted when the reservation is finalized.
type TicketConfirmedPayload struct {
	ReservationID string `json:"reservation_id"`
	EventID       string `json:"event_id"`
	Quantity      int    `json:"quantity"`
}

// TicketReleasedPayload is emitted whenever a held seat returns to the
// pool (expired, payment failed or user cancelled).
type TicketReleasedPayload struct {
	ReservationID string `json:"reservation_id"`
	EventID       string `json:"event_id"`
	Quantity      int    `json:"quantity"`
	Reason        string `json:"reason"`
}
