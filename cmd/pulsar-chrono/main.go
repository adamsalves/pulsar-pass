package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/chrono"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
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

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetVersion(version.Version)
	healthServer.SetReady(true)

	sweeper := chrono.NewSweeper(nil, nil, log, cfg.SweepInterval, cfg.SweepBatch)
	go sweeper.Run(ctx)

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
	log.Info("reservation source and broker adapters are wired in the next cycle; sweeper is idle")

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return healthServer.Shutdown(shutdownCtx)
}
