package holds

import (
	"sync"
	"time"
)

// OpOutcome classifies one hold operation for metrics.
type OpOutcome int

const (
	// OpSuccess reached Redis and returned without error.
	OpSuccess OpOutcome = iota
	// OpDegraded reached Redis and failed; the flow continues without
	// the fast path.
	OpDegraded
	// OpShortCircuited was rejected by the open breaker without paying
	// a round trip.
	OpShortCircuited
)

// Observer receives hold store events for metrics exposition. The
// observability cycle wires an OTel-backed implementation into the
// service binaries; until then no production consumer exists and the
// store pays nothing for instrumentation when no observer is wired.
//
// Implementations must be safe for concurrent use.
type Observer interface {
	// ObserveOp records one operation attempt (set, release or exists).
	ObserveOp(op string, latency time.Duration, outcome OpOutcome)
	// BreakerOpened records the breaker opening after consecutive
	// failures.
	BreakerOpened()
	// BreakerRecovered records the breaker closing on a successful
	// operation.
	BreakerRecovered()
}

// WithObserver wires a metrics observer into the store. A nil observer
// keeps the store uninstrumented.
func WithObserver(obs Observer) Option {
	return func(s *settings) {
		s.observer = obs
	}
}

// OpCounters aggregates every recorded attempt of one operation kind.
type OpCounters struct {
	Attempts       int64
	Succeeded      int64
	Degraded       int64
	ShortCircuited int64
}

// Snapshot is the aggregated state recorded by a SnapshotObserver.
type Snapshot struct {
	Ops map[string]OpCounters

	// Latency accounting covers only the operations that reached Redis
	// (successes and degradations); short-circuited attempts pay no
	// round trip. Bucketed percentiles (p99) belong to the observability
	// cycle, which will feed a real histogram from the per-op events.
	LatencyCount int64
	LatencySum   time.Duration
	LatencyMax   time.Duration

	BreakerOpen           bool
	BreakerOpenedCount    int64
	BreakerRecoveredCount int64
}

// SnapshotObserver is a dependency-free Observer aggregating counters
// in memory, for tests and introspection while no exposition endpoint
// exists.
type SnapshotObserver struct {
	mu   sync.Mutex
	snap Snapshot
}

// NewSnapshotObserver returns an empty, ready-to-wire observer.
func NewSnapshotObserver() *SnapshotObserver {
	return &SnapshotObserver{snap: Snapshot{Ops: make(map[string]OpCounters)}}
}

// ObserveOp implements Observer.
func (o *SnapshotObserver) ObserveOp(op string, latency time.Duration, outcome OpOutcome) {
	o.mu.Lock()
	defer o.mu.Unlock()
	c := o.snap.Ops[op]
	c.Attempts++
	switch outcome {
	case OpSuccess:
		c.Succeeded++
		o.snap.LatencyCount++
		o.snap.LatencySum += latency
		if latency > o.snap.LatencyMax {
			o.snap.LatencyMax = latency
		}
	case OpDegraded:
		c.Degraded++
		o.snap.LatencyCount++
		o.snap.LatencySum += latency
		if latency > o.snap.LatencyMax {
			o.snap.LatencyMax = latency
		}
	case OpShortCircuited:
		c.ShortCircuited++
	}
	o.snap.Ops[op] = c
}

// BreakerOpened implements Observer.
func (o *SnapshotObserver) BreakerOpened() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snap.BreakerOpen = true
	o.snap.BreakerOpenedCount++
}

// BreakerRecovered implements Observer.
func (o *SnapshotObserver) BreakerRecovered() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snap.BreakerOpen = false
	o.snap.BreakerRecoveredCount++
}

// Stats returns a copy of the aggregated state.
func (o *SnapshotObserver) Stats() Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := o.snap
	out.Ops = make(map[string]OpCounters, len(o.snap.Ops))
	for op, c := range o.snap.Ops {
		out.Ops[op] = c
	}
	return out
}
