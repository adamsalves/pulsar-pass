package chrono

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
)

// sweepMetrics holds the lazily created instruments: the package
// initializes before the binaries call metrics.Init, so instruments
// bind on first use to the provider in place at that moment — without
// Init they bind to the no-op default and cost nothing.
type sweepMetrics struct {
	once       sync.Once
	batches    api.Int64Counter
	expired    api.Int64Counter
	pendingAge api.Float64Gauge
	err        error
}

var swMetrics sweepMetrics

func (m *sweepMetrics) instruments() (batches, expired api.Int64Counter, pendingAge api.Float64Gauge, err error) {
	m.once.Do(func() {
		meter := otel.Meter("pulsar-pass/chrono")
		m.batches, m.err = meter.Int64Counter("pulsar_chrono_sweeps_total",
			api.WithDescription("Sweep pass outcomes"))
		if m.err != nil {
			return
		}
		m.expired, m.err = meter.Int64Counter("pulsar_chrono_reservations_expired_total",
			api.WithDescription("Compensation events published for expired reservations"))
		if m.err != nil {
			return
		}
		m.pendingAge, m.err = meter.Float64Gauge("pulsar_chrono_pending_max_age_seconds",
			api.WithDescription("Age of the oldest PENDING reservation"))
	})
	return m.batches, m.expired, m.pendingAge, m.err
}

// PendingAgeSource is the optional source of the oldest PENDING
// reservation age, feeding the backlog gauge from the observability
// blueprint. Sources without it simply skip the gauge.
type PendingAgeSource interface {
	// MaxPendingAge reports the age of the oldest PENDING reservation;
	// false when none is pending.
	MaxPendingAge(ctx context.Context) (time.Duration, bool)
}

// recordSweepOutcome counts a sweep pass result.
func recordSweepOutcome(sweepErr error, expired int) {
	batches, expiredCounter, _, err := swMetrics.instruments()
	if err != nil {
		return
	}
	ctx := context.Background()
	outcome := "ok"
	if sweepErr != nil {
		outcome = "error"
	}
	batches.Add(ctx, 1, api.WithAttributes(attribute.String("outcome", outcome)))
	if expired > 0 {
		expiredCounter.Add(ctx, int64(expired))
	}
}

// recordPendingAge reports the age of the oldest PENDING reservation;
// the gauge is omitted when nothing is pending (no stale backlog).
func recordPendingAge(ctx context.Context, source ReservationSource) {
	if _, _, gauge, err := swMetrics.instruments(); err != nil || gauge == nil {
		return
	}
	ageSource, ok := source.(PendingAgeSource)
	if !ok {
		return
	}
	if age, ok := ageSource.MaxPendingAge(ctx); ok {
		swMetrics.pendingAge.Record(ctx, age.Seconds())
	}
}
