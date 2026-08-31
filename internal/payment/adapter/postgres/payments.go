// Package postgres implements the payment application ports on top of
// PostgreSQL using pgx.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adamsalves/pulsar-pass/internal/payment"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

// Payments is the payment.PaymentRepository backed by PostgreSQL.
type Payments struct {
	pool *pgxpool.Pool
}

// NewPayments builds the repository.
func NewPayments(pool *pgxpool.Pool) *Payments {
	return &Payments{pool: pool}
}

// Create persists a pending payment attempt.
func (r *Payments) Create(ctx context.Context, p *payment.Payment) error {
	q := pgtx.QuerierFrom(ctx, r.pool)
	_, err := q.Exec(ctx, `
		INSERT INTO payments
		    (id, reservation_id, user_id, amount_cents, currency, status, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		p.ID, p.ReservationID, p.UserID, p.AmountCents, p.Currency,
		string(p.Status), p.IdempotencyKey)
	return err
}

// UpdateStatus transitions a payment and records the acquirer result.
func (r *Payments) UpdateStatus(ctx context.Context, id string, status payment.PaymentStatus, gatewayRef, failureReason string) error {
	q := pgtx.QuerierFrom(ctx, r.pool)
	_, err := q.Exec(ctx, `
		UPDATE payments
		   SET status         = $2,
		       gateway_ref    = $3,
		       failure_reason = $4,
		       updated_at     = now()
		 WHERE id = $1`, id, string(status), gatewayRef, failureReason)
	return err
}
