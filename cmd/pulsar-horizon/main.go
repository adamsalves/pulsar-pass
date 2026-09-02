package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/broker"
	"github.com/adamsalves/pulsar-pass/internal/horizon"
	horizonadapter "github.com/adamsalves/pulsar-pass/internal/horizon/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/metrics"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
	"github.com/adamsalves/pulsar-pass/pkg/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pulsar-horizon:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := horizon.LoadConfig()
	log := logger.New(cfg.Env)

	metricsHandler, stopMetrics, err := metrics.Init(ctx, "pulsar-horizon")
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer func() { _ = stopMetrics(context.Background()) }()

	corePool, err := pgpool.New(ctx, cfg.CoreDBURL, pgpool.Options{MaxConns: 5})
	if err != nil {
		return fmt.Errorf("connect core database: %w", err)
	}
	defer corePool.Close()

	paymentPool, err := pgpool.New(ctx, cfg.PaymentDBURL, pgpool.Options{MaxConns: 5})
	if err != nil {
		return fmt.Errorf("connect payment database: %w", err)
	}
	defer paymentPool.Close()

	bus, err := broker.Connect(ctx, cfg.NATSURL, log)
	if err != nil {
		return err
	}

	coreRelay := horizon.NewRelay(horizonadapter.NewStore(corePool), bus, log, cfg.PollInterval, cfg.RelayBatch)
	go coreRelay.Run(ctx)

	paymentRelay := horizon.NewRelay(horizonadapter.NewStore(paymentPool), bus, log, cfg.PollInterval, cfg.RelayBatch)
	go paymentRelay.Run(ctx)

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetVersion(version.Version)
	healthServer.SetReady(true)
	healthServer.Mount("/metrics", metricsHandler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-horizon started",
		"version", version.Version,
		"env", cfg.Env,
		"health_addr", cfg.HealthAddr,
		"poll_interval", cfg.PollInterval.String(),
		"relay_batch", cfg.RelayBatch,
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
