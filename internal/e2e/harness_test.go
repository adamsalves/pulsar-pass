// Package e2e runs the reservation saga against real infrastructure:
// PostgreSQL (core and payment databases), Redis and a JetStream
// broker, wiring the same service components that production binaries
// assemble. The gateway HTTP layer, the core consumers, the payment
// projection/processor, the outbox relay and the TTL sweeper all run
// for real; only the acquirer is simulated.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
	natsserver "github.com/nats-io/nats-server/v2/server"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/adamsalves/pulsar-pass/internal/broker"
	"github.com/adamsalves/pulsar-pass/internal/chrono"
	chronoadapter "github.com/adamsalves/pulsar-pass/internal/chrono/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/core"
	pgadapter "github.com/adamsalves/pulsar-pass/internal/core/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/internal/gateway"
	"github.com/adamsalves/pulsar-pass/internal/holds"
	"github.com/adamsalves/pulsar-pass/internal/horizon"
	horizonadapter "github.com/adamsalves/pulsar-pass/internal/horizon/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/payment"
	payadapter "github.com/adamsalves/pulsar-pass/internal/payment/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

const (
	reservationTTL = 2 * time.Minute
	expirationTTL  = 10 * time.Second
	relayInterval  = 20 * time.Millisecond
	sweepInterval  = 50 * time.Millisecond
	e2eTimeout     = 20 * time.Second
)

// harness boots the whole saga topology for one test.
type harness struct {
	corePool *pgxpool.Pool
	payPool  *pgxpool.Pool
	bus      *eventbus.JetStream
	holds    *holds.Store
	api      *httptest.Server
}

func boot(t *testing.T) *harness {
	t.Helper()
	return bootTTL(t, reservationTTL)
}

// bootTTL boots the saga with a custom reservation TTL, letting the
// expiration scenario run against the natural sweep path instead of
// forcing expires_at backwards (which would desync the payment
// projection from the core, a divergence that cannot occur in
// production because both sides derive from the same ticket.reserved).
func bootTTL(t *testing.T, ttl time.Duration) *harness {
	t.Helper()
	log := logger.New("test")
	ctx, cancel := context.WithCancel(context.Background())

	corePool, payPool := startDatabases(t)
	holdStore := holds.New(strings.TrimPrefix(startRedis(t), "redis://"), log)
	t.Cleanup(func() { _ = holdStore.Close() })

	bus, err := broker.Connect(ctx, startNATS(t), log)
	if err != nil {
		t.Fatalf("connect broker: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })

	// pulsar-core: authoritative inventory + reservation state machine.
	coreService := application.NewReservationService(
		pgadapter.NewReservations(corePool),
		pgadapter.NewInventory(corePool),
		pgadapter.NewOutbox(corePool),
		pgtx.NewManager(corePool),
		application.SystemClock{},
		ttl,
		holdStore,
	)
	if err := core.NewSubscribers(coreService, log).Register(bus); err != nil {
		t.Fatalf("register core subscribers: %v", err)
	}

	// pulsar-payment: projection + charge with the deterministic
	// simulated acquirer (FailureRate 0; token "fail-me" forces decline).
	contexts := payadapter.NewContexts(payPool)
	processor := payment.NewProcessor(
		payadapter.NewPayments(payPool),
		contexts,
		payadapter.NewOutbox(payPool),
		pgtx.NewManager(payPool),
		&payment.SimulatedAcquirer{FailureRate: 0},
		payment.SystemClock{},
		log,
	)
	if err := payment.NewSubscribers(processor, contexts, log).Register(bus); err != nil {
		t.Fatalf("register payment subscribers: %v", err)
	}

	// pulsar-horizon: drains both transactional outboxes into the broker.
	go horizon.NewRelay(horizonadapter.NewStore(corePool), bus, log, relayInterval, 100).Run(ctx)
	go horizon.NewRelay(horizonadapter.NewStore(payPool), bus, log, relayInterval, 100).Run(ctx)

	// pulsar-chrono: TTL sweeper, the compensation safety net.
	sweeper := chrono.NewSweeper(chronoadapter.NewSource(corePool), bus, holdStore, log, sweepInterval, 100)
	go sweeper.Run(ctx)

	// pulsar-gateway: the HTTP ingress under test.
	api := httptest.NewServer(gateway.Routes(gateway.NewReservationHandler(bus, log, 8), log))
	t.Cleanup(api.Close)

	// Registered last so it runs first on cleanup: background loops
	// must stop before pools and the broker are torn down.
	t.Cleanup(cancel)

	return &harness{corePool: corePool, payPool: payPool, bus: bus, holds: holdStore, api: api}
}

func startDatabases(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("pulsar_core_e2e"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	coreDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("core dsn: %v", err)
	}
	payDSN := strings.Replace(coreDSN, "pulsar_core_e2e", "pulsar_payment_e2e", 1)
	if err := createDatabase(ctx, coreDSN, "pulsar_payment_e2e"); err != nil {
		t.Fatalf("create payment database: %v", err)
	}

	applyMigrations(t, ctx, coreDSN, []string{
		"../../migrations/core/000001_init.up.sql",
		"../../migrations/core/000002_event_price.up.sql",
	})
	applyMigrations(t, ctx, payDSN, []string{
		"../../migrations/payment/000001_init.up.sql",
		"../../migrations/payment/000002_reservation_context.up.sql",
	})

	corePool, err := pgpool.New(ctx, coreDSN, pgpool.Options{MaxConns: 8})
	if err != nil {
		t.Fatalf("core pool: %v", err)
	}
	t.Cleanup(corePool.Close)
	payPool, err := pgpool.New(ctx, payDSN, pgpool.Options{MaxConns: 8})
	if err != nil {
		t.Fatalf("payment pool: %v", err)
	}
	t.Cleanup(payPool.Close)
	return corePool, payPool
}

func createDatabase(ctx context.Context, adminDSN, name string) error {
	conn, err := pgxConnect(ctx, adminDSN)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", name))
	return err
}

func applyMigrations(t *testing.T, ctx context.Context, dsn string, files []string) {
	t.Helper()
	conn, err := pgxConnect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for migrations: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read migration %s: %v", f, err)
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", f, err)
		}
	}
}

// pgxConnect opens a plain connection; multi-statement migration files
// require the simple query protocol.
func pgxConnect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, cfg)
}

func startRedis(t *testing.T) string {
	t.Helper()
	ctr, err := tcredis.Run(context.Background(), "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	url, err := ctr.ConnectionString(context.Background())
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}
	return url
}

func startNATS(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		JetStream: true,
		Port:      -1,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create embedded nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// waitFor polls cond until it holds or the e2e deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(e2eTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// createReservation drives POST /v1/reservations and returns the
// reservation id the gateway handed back together with the owner
// identity that must pay for it.
func (h *harness) createReservation(t *testing.T, eventID string, quantity int) (reservationID, userID string) {
	t.Helper()
	userID = "user-" + uid.New()
	body, err := json.Marshal(map[string]any{"event_id": eventID, "quantity": quantity})
	if err != nil {
		t.Fatalf("marshal create body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, h.api.URL+"/v1/reservations", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uid.New())
	req.Header.Set("X-User-Id", userID)

	resp, err := h.api.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /v1/reservations: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /v1/reservations status = %d, want 202", resp.StatusCode)
	}
	var out struct {
		Status        string `json:"status"`
		ReservationID string `json:"reservation_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if out.Status != "accepted" || out.ReservationID == "" {
		t.Fatalf("unexpected create response %+v", out)
	}
	return out.ReservationID, userID
}

// payReservation drives POST /v1/reservations/{id}/payment as the
// given user, who is expected to be the reservation owner unless the
// test is exercising an impostor attempt.
func (h *harness) payReservation(t *testing.T, userID, reservationID, token string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"payment_method_token": token})
	if err != nil {
		t.Fatalf("marshal payment body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, h.api.URL+"/v1/reservations/"+reservationID+"/payment", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build payment request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uid.New())
	req.Header.Set("X-User-Id", userID)

	resp, err := h.api.Client().Do(req)
	if err != nil {
		t.Fatalf("POST payment: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST payment status = %d, want 202", resp.StatusCode)
	}
}

func (h *harness) seedEvent(t *testing.T, capacity int) string {
	t.Helper()
	var id string
	err := h.corePool.QueryRow(context.Background(), `
		INSERT INTO events (name, venue, starts_at, sale_opens_at, price_cents, capacity)
		VALUES ('Show E2E', 'Arena', now() + interval '24 hours', now() - interval '1 hour', 1000, $1)
		RETURNING id`, capacity).Scan(&id)
	if err != nil {
		t.Fatalf("seed event: %v", err)
	}
	return id
}

func (h *harness) reservationStatus(t *testing.T, reservationID string) string {
	t.Helper()
	var status string
	err := h.corePool.QueryRow(context.Background(),
		`SELECT status FROM reservations WHERE id = $1`, reservationID).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

func (h *harness) eventCounts(t *testing.T, eventID string) (reserved, sold int) {
	t.Helper()
	err := h.corePool.QueryRow(context.Background(),
		`SELECT reserved_count, sold_count FROM events WHERE id = $1`, eventID).Scan(&reserved, &sold)
	if err != nil {
		t.Fatalf("load event counts: %v", err)
	}
	return reserved, sold
}

func (h *harness) paymentStatus(t *testing.T, reservationID string) string {
	t.Helper()
	var status string
	err := h.payPool.QueryRow(context.Background(),
		`SELECT status FROM payments WHERE reservation_id = $1`, reservationID).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

// paymentCount counts payment attempts recorded for a reservation.
func (h *harness) paymentCount(t *testing.T, reservationID string) int {
	t.Helper()
	var n int
	err := h.payPool.QueryRow(context.Background(),
		`SELECT count(*) FROM payments WHERE reservation_id = $1`, reservationID).Scan(&n)
	if err != nil {
		t.Fatalf("count payments: %v", err)
	}
	return n
}

func (h *harness) contextAmount(t *testing.T, reservationID string) (int64, bool) {
	t.Helper()
	var amount int64
	err := h.payPool.QueryRow(context.Background(),
		`SELECT amount_cents FROM reservation_context WHERE reservation_id = $1`, reservationID).Scan(&amount)
	if err != nil {
		return 0, false
	}
	return amount, true
}

func (h *harness) holdExists(reservationID string) bool {
	return h.holds.Exists(context.Background(), reservationID)
}

// unprocessedOutbox counts outbox rows the relay has not published yet.
func (h *harness) unprocessedOutbox(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE processed_at IS NULL`).Scan(&n)
	if err != nil {
		t.Fatalf("count unprocessed outbox: %v", err)
	}
	return n
}
