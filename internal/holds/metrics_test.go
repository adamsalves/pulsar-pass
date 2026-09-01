package holds_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/holds"
)

// TestSnapshotObserverCountsDegradedAndShortCircuits covers the
// accounting of a continuous outage: every failed attempt before the
// threshold is degraded, everything after is short-circuited, and the
// breaker transition is recorded exactly once.
func TestSnapshotObserverCountsDegradedAndShortCircuits(t *testing.T) {
	obs := holds.NewSnapshotObserver()
	store := holds.New("127.0.0.1:1", slog.Default(),
		holds.WithBreaker(2, 10*time.Second), holds.WithObserver(obs))
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := store.Set(ctx, "res-metrics", time.Minute); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}
	if err := store.Release(ctx, "res-metrics"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if store.Exists(ctx, "res-metrics") {
		t.Fatal("Exists() must report false while degraded")
	}

	stats := obs.Stats()
	set := stats.Ops[holds.OpSet]
	if set.Attempts != 5 {
		t.Errorf("set attempts = %d, want 5", set.Attempts)
	}
	if set.Degraded != 2 {
		t.Errorf("set degraded = %d, want 2 (the breaker threshold)", set.Degraded)
	}
	if set.ShortCircuited != 3 {
		t.Errorf("set short-circuited = %d, want 3 (post-threshold attempts)", set.ShortCircuited)
	}
	if set.Succeeded != 0 {
		t.Errorf("set succeeded = %d, want 0", set.Succeeded)
	}
	release := stats.Ops[holds.OpRelease]
	if release.Attempts != 1 || release.ShortCircuited != 1 {
		t.Errorf("release counters = %+v, want 1 attempt, short-circuited", release)
	}
	exists := stats.Ops[holds.OpExists]
	if exists.Attempts != 1 || exists.ShortCircuited != 1 {
		t.Errorf("exists counters = %+v, want 1 attempt, short-circuited", exists)
	}
	if !stats.BreakerOpen {
		t.Error("breaker must be reported as open")
	}
	if stats.BreakerOpenedCount != 1 || stats.BreakerRecoveredCount != 0 {
		t.Errorf("transitions = (%d opened, %d recovered), want (1, 0)", stats.BreakerOpenedCount, stats.BreakerRecoveredCount)
	}
	// Only the attempts that reached Redis count latency; short-circuits
	// pay no round trip.
	if stats.LatencyCount != 2 {
		t.Errorf("latency count = %d, want 2 (degraded attempts only)", stats.LatencyCount)
	}
	if stats.LatencyMax <= 0 {
		t.Errorf("latency max = %v, want > 0", stats.LatencyMax)
	}
}

// TestSnapshotObserverRecordsSuccessAndRecovery covers the healthy
// path: successes are counted with latency and a breaker that opened
// against a failing endpoint is reported as recovered after a
// successful probe.
func TestSnapshotObserverRecordsSuccessAndRecovery(t *testing.T) {
	obs := holds.NewSnapshotObserver()
	addr := redisAddr(t, startRedis(t))
	store := holds.New(addr, slog.Default(),
		holds.WithBreaker(1, 100*time.Millisecond), holds.WithObserver(obs))
	defer func() { _ = store.Close() }()

	// A canceled context fails the operation without depending on the
	// endpoint, opening the breaker while Redis itself is healthy.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Set(ctx, "res-metrics-ok", time.Minute); err != nil {
		t.Fatalf("canceled Set() error = %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if err := store.Set(context.Background(), "res-metrics-ok", time.Minute); err != nil {
		t.Fatalf("Set() after recovery error = %v", err)
	}
	if !store.Exists(context.Background(), "res-metrics-ok") {
		t.Fatal("hold must exist after a recovered store writes it")
	}

	stats := obs.Stats()
	set := stats.Ops[holds.OpSet]
	if set.Degraded != 1 || set.Succeeded != 1 {
		t.Errorf("set counters = %+v, want 1 degraded + 1 succeeded", set)
	}
	exists := stats.Ops[holds.OpExists]
	if exists.Succeeded != 1 {
		t.Errorf("exists counters = %+v, want 1 succeeded", exists)
	}
	if stats.BreakerOpen {
		t.Error("breaker must be reported as closed after recovery")
	}
	if stats.BreakerOpenedCount != 1 || stats.BreakerRecoveredCount != 1 {
		t.Errorf("transitions = (%d opened, %d recovered), want (1, 1)", stats.BreakerOpenedCount, stats.BreakerRecoveredCount)
	}
	if stats.LatencyMax <= 0 {
		t.Errorf("latency max = %v, want > 0", stats.LatencyMax)
	}
}

// TestStatsReturnsACopy: mutations of a returned Snapshot must not
// reach the observer's internal state.
func TestStatsReturnsACopy(t *testing.T) {
	obs := holds.NewSnapshotObserver()
	obs.ObserveOp("set", time.Millisecond, holds.OpSuccess)

	stats := obs.Stats()
	stats.Ops[holds.OpSet] = holds.OpCounters{}
	stats.BreakerOpen = true

	fresh := obs.Stats()
	if fresh.Ops["set"].Succeeded != 1 {
		t.Errorf("succeeded = %+v, want 1 (snapshot must be a copy)", fresh.Ops["set"])
	}
	if fresh.BreakerOpen {
		t.Error("breaker open leaked into the observer state")
	}
}

// TestStoreWithoutObserverPaysNothing: the default wiring keeps the
// store uninstrumented and fully functional.
func TestStoreWithoutObserverPaysNothing(t *testing.T) {
	addr := redisAddr(t, startRedis(t))
	store := holds.New(addr, slog.Default())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Set(ctx, "res-no-obs", time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if !store.Exists(ctx, "res-no-obs") {
		t.Fatal("hold must exist without an observer wired")
	}
}

// The concurrency hammer from holds_test with a snapshot observer
// wired: the observer is shared state too, so the race detector must
// stay silent.
func TestSnapshotObserverUnderConcurrency(t *testing.T) {
	obs := holds.NewSnapshotObserver()
	store := holds.New("127.0.0.1:1", slog.Default(),
		holds.WithBreaker(5, 20*time.Millisecond), holds.WithObserver(obs))
	defer func() { _ = store.Close() }()

	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			id := "res-metrics-concurrent"
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

	stats := obs.Stats()
	var attempts int64
	for _, c := range stats.Ops {
		attempts += c.Attempts
	}
	if attempts == 0 {
		t.Error("no attempts recorded by the concurrent workers")
	}
}
