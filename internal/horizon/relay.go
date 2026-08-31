package horizon

import (
	"context"
	"log/slog"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
)

// OutboxRecord is a row drained from an outbox_events table.
type OutboxRecord struct {
	ID      string
	Subject string
	Payload []byte
	Headers map[string]string
}

// OutboxStore reads and acknowledges outbox rows. FetchBatch is a plain
// read; crash-safety comes from publishing with the outbox event id as
// the broker deduplication key. JetStream dedup only covers a finite
// window (Duplicates, 2 min), so a crash followed by a longer outage can
// produce duplicate deliveries — downstream consumers are therefore
// idempotent by design.
type OutboxStore interface {
	FetchBatch(ctx context.Context, limit int) ([]OutboxRecord, error)
	MarkProcessed(ctx context.Context, ids ...string) error
}

// Relay moves outbox rows to the broker. Message.ID carries the outbox
// event id so JetStream deduplicates retries after a crash between
// publish and mark.
type Relay struct {
	store     OutboxStore
	bus       eventbus.Publisher
	log       *slog.Logger
	interval  time.Duration
	batchSize int
}

// NewRelay wires the relay loop.
func NewRelay(store OutboxStore, bus eventbus.Publisher, log *slog.Logger, interval time.Duration, batchSize int) *Relay {
	return &Relay{
		store:     store,
		bus:       bus,
		log:       log,
		interval:  interval,
		batchSize: batchSize,
	}
}

// Run blocks until ctx is cancelled, draining on every tick.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.sweep(ctx); err != nil {
				r.log.Error("relay sweep failed", "error", err)
			}
		}
	}
}

func (r *Relay) sweep(ctx context.Context) error {
	if r.store == nil || r.bus == nil {
		return nil
	}
	batch, err := r.store.FetchBatch(ctx, r.batchSize)
	if err != nil {
		return err
	}
	published := make([]string, 0, len(batch))
	for _, rec := range batch {
		msg := eventbus.Message{
			ID:      rec.ID,
			Subject: rec.Subject,
			Payload: rec.Payload,
			Headers: rec.Headers,
		}
		if err := r.bus.Publish(ctx, msg); err != nil {
			// Rows already published in this batch are marked; the rest
			// stays for the next sweep. The error is loud: a failing row
			// sits at the head of every batch (ordered by created_at) and
			// must be visible in logs and metrics, not silently retried.
			r.log.Error("outbox publish failed; aborting batch for retry",
				"record_id", rec.ID,
				"subject", rec.Subject,
				"error", err,
			)
			break
		}
		published = append(published, rec.ID)
	}
	if len(published) == 0 {
		return nil
	}
	if err := r.store.MarkProcessed(ctx, published...); err != nil {
		return err
	}
	r.log.Debug("outbox batch relayed", "count", len(published))
	return nil
}
