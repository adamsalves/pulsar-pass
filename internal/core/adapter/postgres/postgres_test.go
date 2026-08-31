package postgres_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/adamsalves/pulsar-pass/internal/core/domain"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
)

var (
	corePool  *pgxpool.Pool
	coreSetup sync.Once
	coreErr   error
)

// coreTestDB boots one PostgreSQL container for the whole test binary,
// applies the core migrations and returns a shared pool. Each test seeds
// its own rows.
func coreTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	coreSetup.Do(func() {
		ctx := context.Background()
		ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("pulsar_core_test"),
			tcpostgres.WithUsername("test"),
			tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			coreErr = err
			return
		}
		dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			coreErr = err
			return
		}
		if err := applyCoreMigrations(ctx, dsn); err != nil {
			coreErr = err
			return
		}
		corePool, coreErr = pgpool.New(ctx, dsn, pgpool.Options{MaxConns: 16})
	})
	if coreErr != nil {
		t.Fatalf("setup core test database: %v", coreErr)
	}
	return corePool
}

func applyCoreMigrations(ctx context.Context, dsn string) error {
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

func seedEvent(t *testing.T, capacity int) string {
	t.Helper()
	var id string
	err := coreTestDB(t).QueryRow(context.Background(), `
		INSERT INTO events (name, venue, starts_at, sale_opens_at, price_cents, capacity)
		VALUES ('Show Teste', 'Arena', now() + interval '24 hours', now() - interval '1 hour', 1000, $1)
		RETURNING id`, capacity).Scan(&id)
	if err != nil {
		t.Fatalf("seed event: %v", err)
	}
	return id
}

func newTestReservation(t *testing.T, eventID string) *domain.Reservation {
	t.Helper()
	return domain.NewReservation(domain.NewReservationInput{
		EventID:  eventID,
		UserID:   "user-outbox",
		Quantity: 1,
		TTL:      10 * time.Minute,
		Now:      time.Now().UTC(),
	})
}
