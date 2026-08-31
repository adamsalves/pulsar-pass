// Package pgtx propagates pgx transactions through the context so that
// repository adapters transparently run on the surrounding unit of work
// instead of the pool.
package pgtx

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txKey struct{}

// ErrNoTransaction is returned when a caller expects a surrounding
// transaction and none is present.
var ErrNoTransaction = errors.New("pgtx: no transaction in context")

// Querier is implemented by both *pgxpool.Pool and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Manager opens transactions and exposes them to repositories via
// context. Nested WithinTx calls reuse the surrounding transaction.
type Manager struct {
	pool *pgxpool.Pool
}

// NewManager binds the manager to a pool.
func NewManager(pool *pgxpool.Pool) *Manager {
	return &Manager{pool: pool}
}

// WithinTx runs fn inside a transaction, committing on success and
// rolling back on error or panic.
func (m *Manager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	if _, ok := txFrom(ctx); ok {
		return fn(ctx)
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return
		}
		err = tx.Commit(ctx)
	}()
	return fn(WithTx(ctx, tx))
}

// WithTx returns ctx carrying tx.
func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// QuerierFrom returns the transaction carried by ctx when present,
// falling back to fallback (usually the pool).
func QuerierFrom(ctx context.Context, fallback Querier) Querier {
	if tx, ok := txFrom(ctx); ok {
		return tx
	}
	return fallback
}

// TxFromContext exposes the surrounding transaction to callers that need
// it directly.
func TxFromContext(ctx context.Context) (pgx.Tx, error) {
	tx, ok := txFrom(ctx)
	if !ok {
		return nil, ErrNoTransaction
	}
	return tx, nil
}

func txFrom(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}
