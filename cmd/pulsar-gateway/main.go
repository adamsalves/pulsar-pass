package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/broker"
	"github.com/adamsalves/pulsar-pass/internal/gateway"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pulsar-gateway:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := gateway.LoadConfig()
	log := logger.New(cfg.Env)

	bus, err := selectBus(ctx, cfg, log)
	if err != nil {
		return err
	}

	handler := gateway.NewReservationHandler(bus, log, cfg.MaxQuantity)
	apiServer := gateway.NewServer(cfg.HTTPAddr, gateway.Routes(handler, log))
	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetReady(true)

	errCh := make(chan error, 2)
	go func() {
		errCh <- apiServer.ListenAndServe()
	}()
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-gateway started",
		"env", cfg.Env,
		"http_addr", cfg.HTTPAddr,
		"health_addr", cfg.HealthAddr,
		"bus", cfg.BusMode,
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
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return healthServer.Shutdown(shutdownCtx)
}

// selectBus resolves the message bus: JetStream in production, with an
// in-memory fallback in development when the broker is unreachable.
func selectBus(ctx context.Context, cfg gateway.Config, log *slog.Logger) (eventbus.Bus, error) {
	if cfg.BusMode == "memory" {
		return memoryBusWithDevHandlers(log), nil
	}
	if cfg.BusMode != "nats" {
		return nil, fmt.Errorf("unknown BUS_MODE %q (use nats or memory)", cfg.BusMode)
	}
	bus, err := broker.Connect(ctx, cfg.NATSURL, log)
	if err == nil {
		return bus, nil
	}
	if cfg.Env == "production" {
		return nil, err
	}
	log.Warn("NATS unreachable; falling back to in-memory bus", "error", err.Error())
	return memoryBusWithDevHandlers(log), nil
}

func memoryBusWithDevHandlers(log *slog.Logger) eventbus.Bus {
	bus := eventbus.NewMemory()
	logCommands(bus, log, envelope.SubjectReservationReserve)
	logCommands(bus, log, envelope.SubjectPaymentProcess)
	return bus
}

// logCommands registers development-only handlers so the in-memory bus
// accepts published commands before any consumer exists.
func logCommands(bus eventbus.Subscriber, log *slog.Logger, subject string) {
	_ = bus.Subscribe(subject, "", func(_ context.Context, msg eventbus.Message) error {
		log.Info("command received",
			"subject", msg.Subject,
			"message_id", msg.ID,
			"request_id", msg.Headers["X-Request-Id"],
		)
		return nil
	})
}
