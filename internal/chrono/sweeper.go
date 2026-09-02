package chrono

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
)

// ExpiredReservation is a pending reservation whose retention window
// elapsed.
type ExpiredReservation struct {
	ReservationID string
	EventID       string
	UserID        string
	Quantity      int
}

// ReservationSource reads overdue reservations. The PostgreSQL
// implementation must use SELECT ... FOR UPDATE SKIP LOCKED so that
// multiple sweeper instances never claim the same row.
type ReservationSource interface {
	FindExpired(ctx context.Context, limit int) ([]ExpiredReservation, error)
}

// HoldCleaner is the optional Redis fast-path hygiene hook: it deletes
// hold:{reservation_id} once the batch of compensation events is out.
// The key expires on its own, so a failing cleaner only delays key
// reuse; the store caps each call and cleanup runs after publishing,
// so Redis can never delay the release of seats.
type HoldCleaner interface {
	Release(ctx context.Context, reservationID string) error
}

// ReservationExpiredPayload is the compensation event published for each
// overdue reservation.
type ReservationExpiredPayload struct {
	ReservationID string    `json:"reservation_id"`
	EventID       string    `json:"event_id"`
	UserID        string    `json:"user_id"`
	Quantity      int       `json:"quantity"`
	ExpiredAt     time.Time `json:"expired_at"`
}

// Sweeper periodically scans for expired reservations and publishes
// reservation.expired. The Redis TTL is only an accelerator; this sweep
// against PostgreSQL is the guarantee that no seat leaks.
type Sweeper struct {
	finder    ReservationSource
	bus       eventbus.Publisher
	holds     HoldCleaner
	log       *slog.Logger
	interval  time.Duration
	batchSize int
}

// NewSweeper wires the sweeper loop. holds is the optional Redis hold
// cleaner (nil disables it).
func NewSweeper(finder ReservationSource, bus eventbus.Publisher, holds HoldCleaner, log *slog.Logger, interval time.Duration, batchSize int) *Sweeper {
	return &Sweeper{
		finder:    finder,
		bus:       bus,
		holds:     holds,
		log:       log,
		interval:  interval,
		batchSize: batchSize,
	}
}

// Run blocks until ctx is cancelled, sweeping on every tick.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recordPendingAge(ctx, s.finder)
			expired, err := s.sweep(ctx)
			recordSweepOutcome(err, expired)
			if err != nil {
				s.log.Error("sweep failed", "error", err)
			}
		}
	}
}

// sweep publishes one batch of compensation events and returns how
// many reservations expired in this pass; on a mid-batch publish
// failure the count of events already published is preserved so the
// expired counter does not undercount.
func (s *Sweeper) sweep(ctx context.Context) (int, error) {
	if s.finder == nil || s.bus == nil {
		return 0, nil
	}
	expired, err := s.finder.FindExpired(ctx, s.batchSize)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	published := 0
	for _, e := range expired {
		payload, err := json.Marshal(ReservationExpiredPayload{
			ReservationID: e.ReservationID,
			EventID:       e.EventID,
			UserID:        e.UserID,
			Quantity:      e.Quantity,
			ExpiredAt:     now,
		})
		if err != nil {
			return 0, err
		}
		msg := eventbus.Message{
			// Stable id: each reservation expires exactly once (enforced
			// by the state machine), so re-sweeps dedup on the broker.
			ID:      e.ReservationID + "-expired",
			Subject: envelope.SubjectReservationExpired,
			Payload: payload,
			Headers: map[string]string{
				"Correlation-Id": e.ReservationID,
			},
		}
		if err := s.bus.Publish(ctx, msg); err != nil {
			return published, err
		}
		published++
		s.log.Info("reservation expired", "reservation_id", e.ReservationID, "event_id", e.EventID)
	}
	// Hold hygiene runs only after every compensation event of the
	// batch is published: a slow or failing Redis may delay key
	// cleanup, never the release of the seats themselves.
	if s.holds != nil {
		for _, e := range expired {
			_ = s.holds.Release(ctx, e.ReservationID)
		}
	}
	return published, nil
}
