package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/broker"
	"github.com/adamsalves/pulsar-pass/internal/core"
	pgadapter "github.com/adamsalves/pulsar-pass/internal/core/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/internal/holds"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/metrics"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
	"github.com/adamsalves/pulsar-pass/pkg/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pulsar-core:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := core.LoadConfig()
	log := logger.New(cfg.Env)

	metricsHandler, stopMetrics, err := metrics.Init(ctx, "pulsar-core")
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer func() { _ = stopMetrics(context.Background()) }()

	pool, err := pgpool.New(ctx, cfg.DatabaseURL, pgpool.Options{MaxConns: 10})
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	holdObserver, err := holds.NewOTelObserver("pulsar-core")
	if err != nil {
		return fmt.Errorf("build holds observer: %w", err)
	}
	holdStore := holds.New(cfg.RedisAddr, log, holds.WithObserver(holdObserver))
	defer func() { _ = holdStore.Close() }()

	service := application.NewReservationService(
		pgadapter.NewReservations(pool),
		pgadapter.NewInventory(pool),
		pgadapter.NewOutbox(pool),
		pgtx.NewManager(pool),
		application.SystemClock{},
		cfg.ReservationTTL,
		holdStore,
	)

	bus, err := broker.Connect(ctx, cfg.NATSURL, log)
	if err != nil {
		return err
	}

	if err := core.NewSubscribers(service, log).Register(bus); err != nil {
		return err
	}

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetVersion(version.Version)
	healthServer.SetReady(true)
	healthServer.Mount("/metrics", metricsHandler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-core started",
		"version", version.Version,
		"env", cfg.Env,
		"health_addr", cfg.HealthAddr,
		"reservation_ttl", cfg.ReservationTTL.String(),
		"redis_addr", cfg.RedisAddr,
	)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = bus.Close(shutdownCtx)
	return healthServer.Shutdown(shutdownCtx)
}
