package postgres_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
)

var (
	payPool  *pgxpool.Pool
	paySetup sync.Once
	payErr   error
)

// payTestDB boots one PostgreSQL container for the payment test binary,
// applies the payment migrations and returns a shared pool.
func payTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	paySetup.Do(func() {
		ctx := context.Background()
		ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("pulsar_payment_test"),
			tcpostgres.WithUsername("test"),
			tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			payErr = err
			return
		}
		dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			payErr = err
			return
		}
		if err := applyPaymentMigrations(ctx, dsn); err != nil {
			payErr = err
			return
		}
		payPool, payErr = pgpool.New(ctx, dsn, pgpool.Options{MaxConns: 8})
	})
	if payErr != nil {
		t.Fatalf("setup payment test database: %v", payErr)
	}
	return payPool
}

func applyPaymentMigrations(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	for _, path := range []string{
		"../../../../migrations/payment/000001_init.up.sql",
		"../../../../migrations/payment/000002_reservation_context.up.sql",
	} {
		sql, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return err
		}
	}
	return nil
}

func seedContext(t *testing.T, reservationID string) {
	t.Helper()
	_, err := payTestDB(t).Exec(context.Background(), `
		INSERT INTO reservation_context (reservation_id, user_id, amount_cents, currency, expires_at)
		VALUES ($1, 'user-1', 2500, 'BRL', now() + interval '10 minutes')
		ON CONFLICT (reservation_id) DO NOTHING`, reservationID)
	if err != nil {
		t.Fatalf("seed reservation context: %v", err)
	}
}
