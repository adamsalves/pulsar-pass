package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	bus := eventbus.NewMemory()
	logCommands(bus, log, envelope.SubjectReservationReserve)
	logCommands(bus, log, envelope.SubjectReservationPayment)

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
		"bus", "in-memory (NATS wiring next cycle)",
	)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return healthServer.Shutdown(shutdownCtx)
}

// logCommands registers a development-only handler so the in-memory bus
// accepts published commands before the JetStream wiring exists.
func logCommands(bus eventbus.Subscriber, log interface {
	Info(msg string, args ...any)
}, subject string) {
	_ = bus.Subscribe(subject, "", func(_ context.Context, msg eventbus.Message) error {
		log.Info("command received",
			"subject", msg.Subject,
			"message_id", msg.ID,
			"request_id", msg.Headers["X-Request-Id"],
		)
		return nil
	})
}
