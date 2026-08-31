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
	"github.com/adamsalves/pulsar-pass/internal/core"
	pgadapter "github.com/adamsalves/pulsar-pass/internal/core/adapter/postgres"
	"github.com/adamsalves/pulsar-pass/internal/core/application"
	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/health"
	"github.com/adamsalves/pulsar-pass/pkg/logger"
	"github.com/adamsalves/pulsar-pass/pkg/pgpool"
	"github.com/adamsalves/pulsar-pass/pkg/pgtx"
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

	pool, err := pgpool.New(ctx, cfg.DatabaseURL, pgpool.Options{MaxConns: 10})
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	service := application.NewReservationService(
		pgadapter.NewReservations(pool),
		pgadapter.NewInventory(pool),
		pgadapter.NewOutbox(pool),
		pgtx.NewManager(pool),
		application.SystemClock{},
		cfg.ReservationTTL,
	)

	bus, err := broker.Connect(ctx, cfg.NATSURL, log)
	if err != nil {
		return err
	}

	type reservationRef struct {
		ReservationID string `json:"reservation_id"`
	}
	subscribe := func(subject, durable string, handler eventbus.Handler) {
		if err := bus.Subscribe(subject, durable, handler); err != nil {
			stop()
			fmt.Fprintln(os.Stderr, "pulsar-core:", err)
			os.Exit(1)
		}
	}

	// Command: user intent to hold tickets.
	subscribe(envelope.SubjectReservationReserve, "core-reserve", func(ctx context.Context, msg eventbus.Message) error {
		var cmd struct {
			ReservationID string `json:"reservation_id"`
			EventID       string `json:"event_id"`
			UserID        string `json:"user_id"`
			Quantity      int    `json:"quantity"`
		}
		if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
			return fmt.Errorf("decode reserve command: %w", err)
		}
		res, err := service.Reserve(ctx, application.ReserveCommand{
			ReservationID: cmd.ReservationID,
			EventID:       cmd.EventID,
			UserID:        cmd.UserID,
			Quantity:      cmd.Quantity,
		})
		if err != nil {
			return fmt.Errorf("reserve: %w", err)
		}
		log.Info("reservation created",
			"reservation_id", res.ID,
			"event_id", res.EventID,
			"quantity", res.Quantity,
			"expires_at", res.ExpiresAt.Format(time.RFC3339),
		)
		return nil
	})

	// Event: payment approved.
	subscribe(envelope.SubjectPaymentSucceeded, "core-payment-succeeded", func(ctx context.Context, msg eventbus.Message) error {
		var ev reservationRef
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return fmt.Errorf("decode payment.succeeded: %w", err)
		}
		if _, err := service.Confirm(ctx, ev.ReservationID); err != nil {
			return fmt.Errorf("confirm reservation %s: %w", ev.ReservationID, err)
		}
		log.Info("reservation confirmed", "reservation_id", ev.ReservationID)
		return nil
	})

	// Event: payment rejected.
	subscribe(envelope.SubjectPaymentFailed, "core-payment-failed", func(ctx context.Context, msg eventbus.Message) error {
		var ev reservationRef
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return fmt.Errorf("decode payment.failed: %w", err)
		}
		if _, err := service.Fail(ctx, ev.ReservationID); err != nil {
			return fmt.Errorf("fail reservation %s: %w", ev.ReservationID, err)
		}
		log.Info("reservation failed; seat released", "reservation_id", ev.ReservationID)
		return nil
	})

	// Event: retention window elapsed (compensation).
	subscribe(envelope.SubjectReservationExpired, "core-reservation-expired", func(ctx context.Context, msg eventbus.Message) error {
		var ev reservationRef
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			return fmt.Errorf("decode reservation.expired: %w", err)
		}
		if _, err := service.Expire(ctx, ev.ReservationID); err != nil {
			return fmt.Errorf("expire reservation %s: %w", ev.ReservationID, err)
		}
		log.Info("reservation expired; seat released", "reservation_id", ev.ReservationID)
		return nil
	})

	healthServer := health.NewServer(cfg.HealthAddr, log)
	healthServer.SetReady(true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- healthServer.ListenAndServe()
	}()

	log.Info("pulsar-core started",
		"env", cfg.Env,
		"health_addr", cfg.HealthAddr,
		"reservation_ttl", cfg.ReservationTTL.String(),
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
