package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
)

var (
	chronoPool  *pgxpool.Pool
	chronoSetup sync.Once
	chronoErr   error
)

func chronoTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	chronoSetup.Do(func() {
		ctx := context.Background()
		ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("pulsar_chrono_test"),
			tcpostgres.WithUsername("test"),
			tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			chronoErr = err
			return
		}
		dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			chronoErr = err
			return
		}
		if err := applyChronoMigrations(ctx, dsn); err != nil {
			chronoErr = err
			return
		}
		chronoPool, chronoErr = pgpool.New(ctx, dsn, pgpool.Options{MaxConns: 8})
	})
	if chronoErr != nil {
		t.Fatalf("setup chrono test database: %v", chronoErr)
	}
	return chronoPool
}

func applyChronoMigrations(ctx context.Context, dsn string) error {
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

func seedChronoReservation(t *testing.T, eventID, userID, expiresExpr string) string {
	t.Helper()
	q := fmt.Sprintf(`
		INSERT INTO reservations (event_id, user_id, quantity, amount_cents, currency, expires_at)
		VALUES ($1, $2, 1, 1000, 'BRL', %s)
		RETURNING id`, expiresExpr)
	var id string
	err := chronoTestDB(t).QueryRow(context.Background(), q, eventID, userID).Scan(&id)
	if err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	return id
}

func seedChronoEvent(t *testing.T) string {
	t.Helper()
	var id string
	err := chronoTestDB(t).QueryRow(context.Background(), `
		INSERT INTO events (name, venue, starts_at, sale_opens_at, price_cents, capacity)
		VALUES ('Show Chrono', 'Arena', now() + interval '24 hours', now() - interval '1 hour', 1000, 10)
		RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("seed event: %v", err)
	}
	return id
}
