// Package postgres implements the payment application ports on top of
// PostgreSQL using pgx.
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// Create persists a pending payment attempt. A violated idempotency
// unique constraint maps to payment.ErrDuplicatePayment.
func (r *Payments) Create(ctx context.Context, p *payment.Payment) error {
	q := pgtx.QuerierFrom(ctx, r.pool)
	_, err := q.Exec(ctx, `
		INSERT INTO payments
		    (id, reservation_id, user_id, amount_cents, currency, status, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		p.ID, p.ReservationID, p.UserID, p.AmountCents, p.Currency,
		string(p.Status), p.IdempotencyKey)
	if isUniqueViolation(err) {
		return payment.ErrDuplicatePayment
	}
	return err
}

// GetByIdempotencyKey loads an existing attempt for redelivered
// commands.
func (r *Payments) GetByIdempotencyKey(ctx context.Context, key string) (*payment.Payment, error) {
	q := pgtx.QuerierFrom(ctx, r.pool)
	var p payment.Payment
	var status string
	err := q.QueryRow(ctx, `
		SELECT id, reservation_id, user_id, amount_cents, currency, status,
		       idempotency_key, COALESCE(gateway_ref, ''), COALESCE(failure_reason, '')
		  FROM payments
		 WHERE idempotency_key = $1`, key).
		Scan(&p.ID, &p.ReservationID, &p.UserID, &p.AmountCents, &p.Currency,
			&status, &p.IdempotencyKey, &p.GatewayRef, &p.FailureReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, payment.ErrPaymentNotFound
	}
	if err != nil {
		return nil, err
	}
	p.Status = payment.PaymentStatus(status)
	return &p, nil
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
