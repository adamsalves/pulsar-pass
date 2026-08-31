package payment

import (
	"context"
	"time"
)

// Clock abstracts time for testability.
type Clock interface {
	Now() time.Time
}

// SystemClock returns the wall clock in UTC.
type SystemClock struct{}

// Now returns the current UTC time.
func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}

// TxRunner executes fn inside the unit of work of the underlying store.
// Repository calls sharing the context commit atomically.
type TxRunner interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// PaymentRepository persists payment attempts.
type PaymentRepository interface {
	Create(ctx context.Context, p *Payment) error
	// GetByIdempotencyKey loads an existing attempt for redelivered
	// commands, mapping missing rows to ErrPaymentNotFound.
	GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error)
	UpdateStatus(ctx context.Context, id string, status PaymentStatus, gatewayRef, failureReason string) error
}

// ChargeRequest carries the acquirer charge input.
type ChargeRequest struct {
	ReservationID  string
	UserID         string
	AmountCents    int64
	Currency       string
	Token          string
	IdempotencyKey string
}

// ChargeResult carries the acquirer charge output.
type ChargeResult struct {
	GatewayRef string
}

// Acquirer abstracts the external payment gateway.
type Acquirer interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
}

// ReservationContext is the local projection of a reservation owned by
// pulsar-core, mirrored into the payment database from ticket.reserved
// events. Database-per-service stays intact: no cross-service joins.
type ReservationContext struct {
	ReservationID string
	UserID        string
	AmountCents   int64
	Currency      string
	ExpiresAt     time.Time
}

// ReservationContextRepository maintains the local projection.
type ReservationContextRepository interface {
	Upsert(ctx context.Context, rc ReservationContext) error
	Get(ctx context.Context, reservationID string) (ReservationContext, error)
}

// OutboxRecord is a row of the payment service transactional outbox.
type OutboxRecord struct {
	ID            string
	Subject       string
	EventType     string
	EventVersion  int
	Source        string
	CorrelationID string
	CausationID   string
	Payload       []byte
	OccurredAt    time.Time
}

// OutboxRepository enqueues records that pulsar-horizon relays to the
// broker; implementations must make Enqueue callable in the same
// transaction as the payment state change it describes.
type OutboxRepository interface {
	Enqueue(ctx context.Context, records ...OutboxRecord) error
}
