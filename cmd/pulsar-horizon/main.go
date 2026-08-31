package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/horizon"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
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

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetReady(true)

	relay := horizon.NewRelay(nil, nil, log, cfg.PollInterval, cfg.RelayBatch)
	go relay.Run(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-horizon started",
		"env", cfg.Env,
		"health_addr", cfg.HealthAddr,
		"poll_interval", cfg.PollInterval.String(),
		"relay_batch", cfg.RelayBatch,
	)
	log.Info("outbox stores and broker adapters are wired in the next cycle; relay is idle")

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
