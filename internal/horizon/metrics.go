package horizon

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	api "go.opentelemetry.io/otel/metric"
)

// BacklogCounter is the optional capability of an OutboxStore to
// report how many rows are still unprocessed, feeding the outbox relay
// lag gauge from the observability blueprint. Stores without it simply
// skip the gauge.
type BacklogCounter interface {
	CountUnprocessed(ctx context.Context) (int64, error)
}

// relayMetrics holds the lazily created instruments: the package
// initializes before the binaries call metrics.Init, so instruments
// bind on first use to the provider in place at that moment — without
// Init they bind to the no-op default and cost nothing.
type relayMetrics struct {
	once      sync.Once
	published api.Int64Counter
	failures  api.Int64Counter
	lag       api.Int64Gauge
	err       error
}

var rlMetrics relayMetrics

func (m *relayMetrics) instruments() (published, failures api.Int64Counter, lag api.Int64Gauge, err error) {
	m.once.Do(func() {
		meter := otel.Meter("pulsar-pass/horizon")
		m.published, m.err = meter.Int64Counter("pulsar_horizon_events_relayed_total",
			api.WithDescription("Outbox events relayed to the broker"))
		if m.err != nil {
			return
		}
		m.failures, m.err = meter.Int64Counter("pulsar_horizon_relay_failures_total",
			api.WithDescription("Relay sweeps that aborted with a failing record"))
		if m.err != nil {
			return
		}
		m.lag, m.err = meter.Int64Gauge("pulsar_horizon_outbox_backlog",
			api.WithDescription("Unprocessed outbox rows"))
	})
	return m.published, m.failures, m.lag, m.err
}

// recordBacklog reports the unprocessed row count when the store
// supports it.
func recordBacklog(ctx context.Context, store OutboxStore) {
	_, _, lag, err := rlMetrics.instruments()
	if err != nil || lag == nil {
		return
	}
	counter, ok := store.(BacklogCounter)
	if !ok {
		return
	}
	if n, err := counter.CountUnprocessed(ctx); err == nil {
		lag.Record(ctx, n)
	}
}

// recordSweepOutcome counts the events relayed by one sweep and whether
// it aborted on a failing record.
func recordSweepOutcome(published int, failed bool) {
	publishedCounter, failures, _, err := rlMetrics.instruments()
	if err != nil {
		return
	}
	ctx := context.Background()
	if published > 0 {
		publishedCounter.Add(ctx, int64(published))
	}
	if failed {
		failures.Add(ctx, 1)
	}
}
