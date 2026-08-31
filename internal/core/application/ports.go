// Package application holds the ports (interfaces) that adapters must
// implement and the orchestration of the reservation use cases.
package application

import (
	"context"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core/domain"
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
// Repository adapters participating in the same context share the
// transaction, which is what makes state change + outbox atomic.
type TxRunner interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// ReservationRepository persists reservation aggregates.
type ReservationRepository interface {
	Create(ctx context.Context, r *domain.Reservation) error
	Get(ctx context.Context, id string) (*domain.Reservation, error)
	Update(ctx context.Context, r *domain.Reservation) error
}

// InventoryRepository reads events and mutates their capacity
// atomically. Implementations must use conditional updates so that
// concurrent consumers can never oversell (zero overbooking).
type InventoryRepository interface {
	// Event loads an event by id, mapping missing rows to
	// domain.ErrNotFound.
	Event(ctx context.Context, id string) (*domain.Event, error)
	// ReserveCapacity consumes n units, returning domain.ErrSoldOut when
	// capacity is exhausted.
	ReserveCapacity(ctx context.Context, eventID string, n int) error
	// ReleaseCapacity returns n reserved units to the pool.
	ReleaseCapacity(ctx context.Context, eventID string, n int) error
	// CommitSold converts n reserved units into sold tickets.
	CommitSold(ctx context.Context, eventID string, n int) error
}

// OutboxRecord is a row of the transactional outbox.
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
// broker. Implementations must make Enqueue callable in the same
// transaction as the state change it describes.
type OutboxRepository interface {
	Enqueue(ctx context.Context, records ...OutboxRecord) error
}
