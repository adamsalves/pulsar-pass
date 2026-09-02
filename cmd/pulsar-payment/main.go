package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/broker"
	"github.com/adamsalves/pulsar-pass/internal/payment"
	pgadapter "github.com/adamsalves/pulsar-pass/internal/payment/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/metrics"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
	"github.com/adamsalves/pulsar-pass/pkg/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pulsar-payment:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := payment.LoadConfig()
	log := logger.New(cfg.Env)

	metricsHandler, stopMetrics, err := metrics.Init(ctx, "pulsar-payment")
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer func() { _ = stopMetrics(context.Background()) }()

	pool, err := pgpool.New(ctx, cfg.DatabaseURL, pgpool.Options{MaxConns: 10})
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	contexts := pgadapter.NewContexts(pool)
	processor := payment.NewProcessor(
		pgadapter.NewPayments(pool),
		contexts,
		pgadapter.NewOutbox(pool),
		pgtx.NewManager(pool),
		&payment.SimulatedAcquirer{
			FailureRate: cfg.SimulatedFailureRate,
			Delay:       cfg.ChargeDelay,
		},
		payment.SystemClock{},
		log,
	)

	bus, err := broker.Connect(ctx, cfg.NATSURL, log)
	if err != nil {
		return err
	}

	if err := payment.NewSubscribers(processor, contexts, log).Register(bus); err != nil {
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

	log.Info("pulsar-payment started",
		"version", version.Version,
		"env", cfg.Env,
		"health_addr", cfg.HealthAddr,
		"simulated_charge_delay", cfg.ChargeDelay.String(),
		"simulated_failure_rate", cfg.SimulatedFailureRate,
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
