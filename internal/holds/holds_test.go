package holds_test

import (
	"context"
	"log/slog"
	"net"
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
	if err := store.Release(ctx, "res-4"); err != nil {
		t.Fatalf("Release() must degrade gracefully, got error %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("degraded calls must fail fast; took %v", time.Since(start))
	}
}
