package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/core"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
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

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetVersion(version.Version)
	healthServer.SetReady(true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-core started",
		"version", version.Version,
		"env", cfg.Env,
		"health_addr", cfg.HealthAddr,
		"reservation_ttl", cfg.ReservationTTL.String(),
	)
	log.Info("repository and broker adapters are wired in the next cycle; service is idle")

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
