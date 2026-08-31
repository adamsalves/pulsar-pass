// Package postgres implements the core application ports on top of
// PostgreSQL using pgx.
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adamsalves/pulsar-pass/internal/core/domain"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

// Inventory is the application.InventoryRepository backed by PostgreSQL.
// Capacity is consumed by a single conditional UPDATE, so concurrent
// consumers can never oversell: the database is the lock.
type Inventory struct {
	pool *pgxpool.Pool
}

// NewInventory builds the repository.
func NewInventory(pool *pgxpool.Pool) *Inventory {
	return &Inventory{pool: pool}
}

// Event loads an event aggregate by id.
func (i *Inventory) Event(ctx context.Context, id string) (*domain.Event, error) {
	q := pgtx.QuerierFrom(ctx, i.pool)
	var e domain.Event
	err := q.QueryRow(ctx, `
		SELECT id, name, venue, starts_at, sale_opens_at, price_cents,
		       capacity, reserved_count, sold_count, created_at, updated_at
		  FROM events
		 WHERE id = $1`, id).
		Scan(&e.ID, &e.Name, &e.Venue, &e.StartsAt, &e.SaleOpensAt, &e.PriceCents,
			&e.Capacity, &e.ReservedCount, &e.SoldCount, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// ReserveCapacity consumes n units; zero affected rows means the event
// does not exist or capacity is exhausted.
func (i *Inventory) ReserveCapacity(ctx context.Context, eventID string, n int) error {
	q := pgtx.QuerierFrom(ctx, i.pool)
	tag, err := q.Exec(ctx, `
		UPDATE events
		   SET reserved_count = reserved_count + $2,
		       updated_at     = now()
		 WHERE id = $1
		   AND reserved_count + sold_count + $2 <= capacity`, eventID, n)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return i.notFoundOr(ctx, eventID, domain.ErrSoldOut)
	}
	return nil
}

// ReleaseCapacity returns n reserved units to the pool.
func (i *Inventory) ReleaseCapacity(ctx context.Context, eventID string, n int) error {
	q := pgtx.QuerierFrom(ctx, i.pool)
	tag, err := q.Exec(ctx, `
		UPDATE events
		   SET reserved_count = reserved_count - $2,
		       updated_at     = now()
		 WHERE id = $1
		   AND reserved_count >= $2`, eventID, n)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return i.notFoundOr(ctx, eventID, domain.ErrNotEnoughReserved)
	}
	return nil
}

// CommitSold converts n reserved units into sold tickets.
func (i *Inventory) CommitSold(ctx context.Context, eventID string, n int) error {
	q := pgtx.QuerierFrom(ctx, i.pool)
	tag, err := q.Exec(ctx, `
		UPDATE events
		   SET reserved_count = reserved_count - $2,
		       sold_count     = sold_count + $2,
		       updated_at     = now()
		 WHERE id = $1
		   AND reserved_count >= $2`, eventID, n)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return i.notFoundOr(ctx, eventID, domain.ErrNotEnoughReserved)
	}
	return nil
}

func (i *Inventory) notFoundOr(ctx context.Context, eventID string, fallback error) error {
	exists, err := i.eventExists(ctx, eventID)
	if err != nil {
		return err
	}
	if !exists {
		return domain.ErrNotFound
	}
	return fallback
}

func (i *Inventory) eventExists(ctx context.Context, eventID string) (bool, error) {
	q := pgtx.QuerierFrom(ctx, i.pool)
	var exists bool
	err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, eventID).Scan(&exists)
	return exists, err
}
