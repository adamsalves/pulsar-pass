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
	horizonPool  *pgxpool.Pool
	horizonSetup sync.Once
	horizonErr   error
)

func horizonTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	horizonSetup.Do(func() {
		ctx := context.Background()
		ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("pulsar_horizon_test"),
			tcpostgres.WithUsername("test"),
			tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			horizonErr = err
			return
		}
		dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			horizonErr = err
			return
		}
		if err := applyHorizonMigrations(ctx, dsn); err != nil {
			horizonErr = err
			return
		}
		horizonPool, horizonErr = pgpool.New(ctx, dsn, pgpool.Options{MaxConns: 8})
	})
	if horizonErr != nil {
		t.Fatalf("setup horizon test database: %v", horizonErr)
	}
	return horizonPool
}

func applyHorizonMigrations(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	for _, path := range []string{
		"../../../../migrations/core/000001_init.up.sql",
		"../../../../migrations/core/000002_event_price.up.sql",
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

func seedOutboxRow(t *testing.T, id, subject string) {
	t.Helper()
	_, err := horizonTestDB(t).Exec(context.Background(), `
		INSERT INTO outbox_events (id, subject, event_type, source, correlation_id, payload)
		VALUES ($1::uuid, $2, 'ticket.reserved', 'pulsar-core', $1::uuid, '{"x":1}'::jsonb)`,
		id, subject)
	if err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}
}
