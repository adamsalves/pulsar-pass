// Package postgres implements the chrono ReservationSource on top of
// PostgreSQL.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adamsalves/pulsar-pass/internal/chrono"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

// Source is the chrono.ReservationSource backed by PostgreSQL. Rows are
// selected with FOR UPDATE SKIP LOCKED inside a short transaction so
// concurrent sweeper instances never claim the same batch.
type Source struct {
	pool *pgxpool.Pool
}

// NewSource builds the reservation source.
func NewSource(pool *pgxpool.Pool) *Source {
	return &Source{pool: pool}
}

// FindExpired returns pending reservations whose retention window
// elapsed. No mutation happens here: pulsar-core performs the state
// transition when it consumes reservation.expired.
func (s *Source) FindExpired(ctx context.Context, limit int) ([]chrono.ExpiredReservation, error) {
	var out []chrono.ExpiredReservation
	manager := pgtx.NewManager(s.pool)
	err := manager.WithinTx(ctx, func(txctx context.Context) error {
		rows, err := pgtx.QuerierFrom(txctx, s.pool).Query(txctx, `
			SELECT id, event_id, user_id, quantity
			  FROM reservations
			 WHERE status = 'PENDING'
			   AND expires_at < now()
			 ORDER BY expires_at
			 LIMIT $1
			 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e chrono.ExpiredReservation
			if err := rows.Scan(&e.ReservationID, &e.EventID, &e.UserID, &e.Quantity); err != nil {
				return err
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
