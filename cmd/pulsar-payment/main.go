package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adamsalves/pulsar-pass/internal/broker"
	"github.com/adamsalves/pulsar-pass/internal/payment"
	pgadapter "github.com/adamsalves/pulsar-pass/internal/payment/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
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

	subscribe := func(subject, durable string, handler eventbus.Handler) {
		if err := bus.Subscribe(subject, durable, handler); err != nil {
			stop()
			fmt.Fprintln(os.Stderr, "pulsar-payment:", err)
			os.Exit(1)
		}
	}

	// Projection: ticket.reserved feeds the local reservation context.
	subscribe(envelope.SubjectTicketReserved, "payment-context", func(ctx context.Context, msg eventbus.Message) error {
		var ev struct {
			ReservationID string    `json:"reservation_id"`
			UserID        string    `json:"user_id"`
			AmountCents   int64     `json:"amount_cents"`
			Currency      string    `json:"currency"`
			ExpiresAt     time.Time `json:"expires_at"`
		}
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return fmt.Errorf("decode ticket.reserved: %w", err)
		}
		if err := contexts.Upsert(ctx, payment.ReservationContext{
			ReservationID: ev.ReservationID,
			UserID:        ev.UserID,
			AmountCents:   ev.AmountCents,
			Currency:      ev.Currency,
			ExpiresAt:     ev.ExpiresAt,
		}); err != nil {
			return fmt.Errorf("upsert reservation context: %w", err)
		}
		log.Info("reservation context stored", "reservation_id", ev.ReservationID)
		return nil
	})

	// Command: user payment submission inside the TTL window.
	subscribe(envelope.SubjectPaymentProcess, "payment-process", func(ctx context.Context, msg eventbus.Message) error {
		var cmd struct {
			ReservationID string `json:"reservation_id"`
			UserID        string `json:"user_id"`
			Token         string `json:"payment_method_token"`
		}
		if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
			return fmt.Errorf("decode payment.process: %w", err)
		}
		idempotencyKey := msg.Headers["Idempotency-Key"]
		if idempotencyKey == "" {
			idempotencyKey = msg.ID
		}
		return processor.Handle(ctx, payment.PaymentRequested{
			ReservationID: cmd.ReservationID,
			UserID:        cmd.UserID,
			Token:         cmd.Token,
		}, idempotencyKey)
	})

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetReady(true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-payment started",
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
