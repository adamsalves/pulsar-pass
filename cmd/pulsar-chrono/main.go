package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/broker"
	"github.com/adamsalves/pulsar-pass/internal/chrono"
	chronoadapter "github.com/adamsalves/pulsar-pass/internal/chrono/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/holds"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/metrics"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
	"github.com/adamsalves/pulsar-pass/pkg/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pulsar-chrono:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := chrono.LoadConfig()
	log := logger.New(cfg.Env)

	metricsHandler, stopMetrics, err := metrics.Init(ctx, "pulsar-chrono")
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer func() { _ = stopMetrics(context.Background()) }()

	pool, err := pgpool.New(ctx, cfg.DatabaseURL, pgpool.Options{MaxConns: 5})
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	bus, err := broker.Connect(ctx, cfg.NATSURL, "pulsar-chrono", log)
	if err != nil {
		return err
	}

	source := chronoadapter.NewSource(pool)
	holdObserver, err := holds.NewOTelObserver("pulsar-chrono")
	if err != nil {
		return fmt.Errorf("build holds observer: %w", err)
	}
	holdStore := holds.New(cfg.RedisAddr, log, holds.WithObserver(holdObserver))
	defer func() { _ = holdStore.Close() }()
	sweeper := chrono.NewSweeper(source, bus, holdStore, log, cfg.SweepInterval, cfg.SweepBatch)
	go sweeper.Run(ctx)

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetVersion(version.Version)
	healthServer.SetReady(true)
	healthServer.Mount("/metrics", metricsHandler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-chrono started",
		"version", version.Version,
		"env", cfg.Env,
		"health_addr", cfg.HealthAddr,
		"sweep_interval", cfg.SweepInterval.String(),
		"sweep_batch", cfg.SweepBatch,
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
