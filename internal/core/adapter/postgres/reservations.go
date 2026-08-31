package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adamsalves/pulsar-pass/internal/core/domain"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
)

// Reservations is the application.ReservationRepository backed by
// PostgreSQL. Updates use optimistic locking on the version column.
type Reservations struct {
	pool *pgxpool.Pool
}

// NewReservations builds the repository.
func NewReservations(pool *pgxpool.Pool) *Reservations {
	return &Reservations{pool: pool}
}

// Create persists a new reservation.
func (r *Reservations) Create(ctx context.Context, res *domain.Reservation) error {
	q := pgtx.QuerierFrom(ctx, r.pool)
	_, err := q.Exec(ctx, `
		INSERT INTO reservations
		    (id, event_id, user_id, status, quantity, amount_cents, currency,
		     expires_at, confirmed_at, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		res.ID, res.EventID, res.UserID, string(res.Status), res.Quantity,
		res.AmountCents, res.Currency, res.ExpiresAt, res.ConfirmedAt,
		res.Version, res.CreatedAt, res.UpdatedAt)
	if isUniqueViolation(err) {
		return domain.ErrConflict
	}
	return err
}

// Get loads a reservation by id.
func (r *Reservations) Get(ctx context.Context, id string) (*domain.Reservation, error) {
	q := pgtx.QuerierFrom(ctx, r.pool)
	var res domain.Reservation
	var status string
	err := q.QueryRow(ctx, `
		SELECT id, event_id, user_id, status, quantity, amount_cents, currency,
		       expires_at, confirmed_at, version, created_at, updated_at
		  FROM reservations
		 WHERE id = $1`, id).
		Scan(&res.ID, &res.EventID, &res.UserID, &status, &res.Quantity,
			&res.AmountCents, &res.Currency, &res.ExpiresAt, &res.ConfirmedAt,
			&res.Version, &res.CreatedAt, &res.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	res.Status = domain.ReservationStatus(status)
	return &res, nil
}

// Update persists a state transition guarded by the previous version.
// Zero affected rows mean a concurrent writer won the race.
func (r *Reservations) Update(ctx context.Context, res *domain.Reservation) error {
	q := pgtx.QuerierFrom(ctx, r.pool)
	tag, err := q.Exec(ctx, `
		UPDATE reservations
		   SET status       = $2,
		       confirmed_at = $3,
		       version      = $4,
		       updated_at   = $5
		 WHERE id = $1
		   AND version = $4 - 1`,
		res.ID, string(res.Status), res.ConfirmedAt, res.Version, res.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrConflict
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
