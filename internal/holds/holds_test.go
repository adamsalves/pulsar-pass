package holds_test

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/adamsalves/pulsar-pass/internal/holds"
)

func startRedis(t *testing.T) string {
	t.Helper()
	ctr, err := tcredis.Run(context.Background(), "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() {
		_ = ctr.Terminate(context.Background())
	})
	url, err := ctr.ConnectionString(context.Background())
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}
	return url
}

// redisAddr strips the redis:// scheme so the store can dial directly.
func redisAddr(t *testing.T, url string) string {
	t.Helper()
	host, port, err := net.SplitHostPort(url)
	if err != nil {
		// ConnectionString returns redis://host:port; drop the scheme.
		const prefix = "redis://"
		if len(url) > len(prefix) {
			return url[len(prefix):]
		}
		t.Fatalf("unexpected redis url %q", url)
	}
	return net.JoinHostPort(host, port)
}

func TestStoreSetAndReleaseLifecycle(t *testing.T) {
	store := holds.New(redisAddr(t, startRedis(t)), slog.Default())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if store.Exists(ctx, "res-1") {
		t.Fatal("hold should not exist before Set")
	}
	if err := store.Set(ctx, "res-1", time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if !store.Exists(ctx, "res-1") {
		t.Fatal("hold should exist after Set")
	}
	if err := store.Release(ctx, "res-1"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if store.Exists(ctx, "res-1") {
		t.Fatal("hold should be gone after Release")
	}
}

func TestStoreSetSkipsNonPositiveTTL(t *testing.T) {
	store := holds.New(redisAddr(t, startRedis(t)), slog.Default())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Set(ctx, "res-2", -time.Second); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if store.Exists(ctx, "res-2") {
		t.Fatal("hold must not be created with a non-positive TTL")
	}
}

func TestStoreDisabledWhenAddressEmpty(t *testing.T) {
	store := holds.New("", slog.Default())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Set(ctx, "res-3", time.Minute); err != nil {
		t.Fatalf("Set() on disabled store error = %v", err)
	}
	if err := store.Release(ctx, "res-3"); err != nil {
		t.Fatalf("Release() on disabled store error = %v", err)
	}
	if store.Exists(ctx, "res-3") {
		t.Fatal("disabled store must never report holds")
	}
}

func TestStoreDegradesGracefullyWhenRedisUnreachable(t *testing.T) {
	store := holds.New("127.0.0.1:1", slog.Default())
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	if err := store.Set(ctx, "res-4", time.Minute); err != nil {
		t.Fatalf("Set() must degrade gracefully, got error %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("degraded Set must stay under the 200ms op cap (with slack); took %v", elapsed)
	}
	start = time.Now()
	if err := store.Release(ctx, "res-4"); err != nil {
		t.Fatalf("Release() must degrade gracefully, got error %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("degraded Release must stay under the 200ms op cap (with slack); took %v", elapsed)
	}
}

// captureHandler records log messages so tests can assert exactly what
// the store emits while degrading.
type captureHandler struct {
	mu       sync.Mutex
	messages []string
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, r.Message)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func (h *captureHandler) count(substr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.messages {
		if strings.Contains(m, substr) {
			n++
		}
	}
	return n
}

// TestStoreBreakerShortCircuitsAfterConsecutiveFailures covers the open
// transition: once the failure threshold is hit, operations stop paying
// the Redis round trip and the per-op warning spam stops.
func TestStoreBreakerShortCircuitsAfterConsecutiveFailures(t *testing.T) {
	logs := &captureHandler{}
	store := holds.New("127.0.0.1:1", slog.New(logs), holds.WithBreaker(3, 10*time.Second))
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		start := time.Now()
		if err := store.Set(ctx, "res-5", time.Minute); err != nil {
			t.Fatalf("Set() must degrade gracefully, got error %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("degraded Set #%d exceeded the op cap (with slack): %v", i+1, elapsed)
		}
	}

	// Breaker is open: the next operations must short-circuit without
	// dialing Redis.
	for i := 0; i < 10; i++ {
		start := time.Now()
		if err := store.Set(ctx, "res-5", time.Minute); err != nil {
			t.Fatalf("short-circuited Set() error = %v", err)
		}
		if err := store.Release(ctx, "res-5"); err != nil {
			t.Fatalf("short-circuited Release() error = %v", err)
		}
		if store.Exists(ctx, "res-5") {
			t.Fatal("short-circuited Exists() must report false")
		}
		if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
			t.Fatalf("open breaker must short-circuit, op took %v", elapsed)
		}
	}

	// Exactly one warning per failure while closed plus a single
	// breaker-open announcement; nothing while open.
	if n := logs.count("redis hold degraded"); n != 3 {
		t.Errorf("degraded warnings = %d, want 3 (one per failure while closed)", n)
	}
	if n := logs.count("breaker open"); n != 1 {
		t.Errorf("breaker-open logs = %d, want 1 (no per-op spam while open)", n)
	}
}

// TestStoreBreakerProbeFailureRearmsCooldown covers the half-open path
// on failure: the first operation after the cooldown reaches Redis, and
// failing it re-arms the window instead of hammering the endpoint.
func TestStoreBreakerProbeFailureRearmsCooldown(t *testing.T) {
	store := holds.New("127.0.0.1:1", slog.Default(), holds.WithBreaker(1, 150*time.Millisecond))
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Set(ctx, "res-6", time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Past the cooldown the next operation is the probe: it pays a real
	// (failing) round trip and re-arms the breaker.
	time.Sleep(250 * time.Millisecond)
	start := time.Now()
	if err := store.Set(ctx, "res-6", time.Minute); err != nil {
		t.Fatalf("probe Set() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("probe must attempt Redis, took %v (looks short-circuited)", elapsed)
	}

	// Still failing: the breaker must be open again, short-circuiting.
	start = time.Now()
	if err := store.Set(ctx, "res-6", time.Minute); err != nil {
		t.Fatalf("short-circuited Set() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("re-armed breaker must short-circuit, op took %v", elapsed)
	}
}

// TestStoreBreakerRecoversOnSuccessfulProbe covers the half-open path
// on success: after the cooldown a probe reaches a healthy Redis,
// closes the breaker and normal operation resumes.
func TestStoreBreakerRecoversOnSuccessfulProbe(t *testing.T) {
	logs := &captureHandler{}
	addr := redisAddr(t, startRedis(t))
	store := holds.New(addr, slog.New(logs), holds.WithBreaker(1, 100*time.Millisecond))
	defer func() { _ = store.Close() }()

	// A canceled context fails the operation without depending on the
	// endpoint, opening the breaker while Redis itself is healthy.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Set(ctx, "res-7", time.Minute); err != nil {
		t.Fatalf("canceled Set() must degrade gracefully, got error %v", err)
	}

	// While open, a healthy Redis is still short-circuited.
	if err := store.Set(context.Background(), "res-7", time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if store.Exists(context.Background(), "res-7") {
		t.Fatal("hold must not exist while the breaker is open")
	}

	// Cooldown elapses: the probe reaches Redis and closes the breaker.
	time.Sleep(200 * time.Millisecond)
	if store.Exists(context.Background(), "res-7") {
		t.Fatal("hold was never written; Exists must be false after recovery")
	}
	if err := store.Set(context.Background(), "res-7", time.Minute); err != nil {
		t.Fatalf("Set() after recovery error = %v", err)
	}
	if !store.Exists(context.Background(), "res-7") {
		t.Fatal("hold must exist after a recovered store writes it")
	}
	if n := logs.count("recovered"); n != 1 {
		t.Errorf("recovery logs = %d, want 1", n)
	}
}

// TestStoreBreakerUnderConcurrency hammers a degraded store from many
// goroutines: the breaker is shared state, so the race detector must
// stay silent and no operation may fail the flow.
func TestStoreBreakerUnderConcurrency(t *testing.T) {
	store := holds.New("127.0.0.1:1", slog.Default(), holds.WithBreaker(5, 20*time.Millisecond))
	defer func() { _ = store.Close() }()

	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			id := "res-concurrent"
			for i := 0; i < 25; i++ {
				if err := store.Set(ctx, id, time.Minute); err != nil {
					t.Errorf("worker %d: Set() error = %v", w, err)
					return
				}
				if err := store.Release(ctx, id); err != nil {
					t.Errorf("worker %d: Release() error = %v", w, err)
					return
				}
				_ = store.Exists(ctx, id)
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}
	wg.Wait()
}

// TestStoreDisabledIgnoresBreakerOptions: the disabled store stays a
// no-op regardless of breaker configuration.
func TestStoreDisabledIgnoresBreakerOptions(t *testing.T) {
	logs := &captureHandler{}
	store := holds.New("", slog.New(logs), holds.WithBreaker(1, time.Second))
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Set(ctx, "res-8", time.Minute); err != nil {
		t.Fatalf("Set() on disabled store error = %v", err)
	}
	if store.Exists(ctx, "res-8") {
		t.Fatal("disabled store must never report holds")
	}
	if n := logs.count("degraded"); n != 0 {
		t.Errorf("disabled store logged %d degradation records, want 0", n)
	}
	if len(logs.messages) != 0 {
		t.Errorf("disabled store logged %d records, want 0", len(logs.messages))
	}
}
