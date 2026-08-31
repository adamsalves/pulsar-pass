package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adamsalves/pulsar-pass/internal/payment"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

// Contexts is the payment.ReservationContextRepository backed by
// PostgreSQL.
type Contexts struct {
	pool *pgxpool.Pool
}

// NewContexts builds the repository.
func NewContexts(pool *pgxpool.Pool) *Contexts {
	return &Contexts{pool: pool}
}

// Upsert mirrors a reservation context from ticket.reserved events.
func (c *Contexts) Upsert(ctx context.Context, rc payment.ReservationContext) error {
	q := pgtx.QuerierFrom(ctx, c.pool)
	_, err := q.Exec(ctx, `
		INSERT INTO reservation_context (reservation_id, user_id, amount_cents, currency, expires_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (reservation_id)
		DO UPDATE SET user_id       = EXCLUDED.user_id,
		              amount_cents  = EXCLUDED.amount_cents,
		              currency      = EXCLUDED.currency,
		              expires_at    = EXCLUDED.expires_at,
		              updated_at    = now()`,
		rc.ReservationID, rc.UserID, rc.AmountCents, rc.Currency, rc.ExpiresAt)
	return err
}

// Get loads the projection, mapping missing rows to
// payment.ErrContextNotFound.
func (c *Contexts) Get(ctx context.Context, reservationID string) (payment.ReservationContext, error) {
	q := pgtx.QuerierFrom(ctx, c.pool)
	var rc payment.ReservationContext
	err := q.QueryRow(ctx, `
		SELECT reservation_id, user_id, amount_cents, currency, expires_at
		  FROM reservation_context
		 WHERE reservation_id = $1`, reservationID).
		Scan(&rc.ReservationID, &rc.UserID, &rc.AmountCents, &rc.Currency, &rc.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return payment.ReservationContext{}, payment.ErrContextNotFound
	}
	if err != nil {
		return payment.ReservationContext{}, err
	}
	return rc, nil
}
