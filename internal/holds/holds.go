// Package holds implements the Redis fast-path accelerator for
// reservation holds (hold:{reservation_id}). PostgreSQL remains the
// authority for the inventory: a missing or failing hold only costs a
// fast-path miss, so every Redis problem degrades to a warning log
// instead of failing the reservation flow.
//
// A built-in circuit breaker bounds the cost of a continuous Redis
// outage: after the failure threshold is reached the breaker opens and
// every operation short-circuits without a round trip until the
// cooldown elapses, when a single half-open probe retries Redis.
package holds

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix namespaces hold keys on the shared Redis instance.
const KeyPrefix = "hold:"

// opTimeout caps every Redis round trip. The hold is an accelerator:
// a slow or unavailable Redis must cost at most this much per
// operation, never the flow itself.
const opTimeout = 200 * time.Millisecond

// Breaker defaults: five consecutive failures open the breaker for
// thirty seconds, turning a continuous outage from ~5 msg/s of
// degraded consumers into a zero-cost fast-path miss.
const (
	defaultFailureThreshold = 5
	defaultCooldown         = 30 * time.Second
)

// Key returns the Redis key that accelerates a reservation.
func Key(reservationID string) string {
	return KeyPrefix + reservationID
}

// Option customizes the hold store.
type Option func(*settings)

// settings carries the tunables resolved from the options.
type settings struct {
	failureThreshold int
	cooldown         time.Duration
}

// WithBreaker tunes the circuit breaker: after failureThreshold
// consecutive failures the store short-circuits for cooldown, then a
// single probe retries Redis. Non-positive values keep the defaults.
func WithBreaker(failureThreshold int, cooldown time.Duration) Option {
	return func(s *settings) {
		if failureThreshold > 0 {
			s.failureThreshold = failureThreshold
		}
		if cooldown > 0 {
			s.cooldown = cooldown
		}
	}
}

// Store is the Redis-backed hold accelerator. A zero client (empty
// address) disables the store: every method is a no-op.
type Store struct {
	client *redis.Client
	log    *slog.Logger
	br     *breaker
}

// New wires the hold store against addr. An empty addr disables the
// accelerator entirely; clients get aggressive timeouts because a slow
// accelerator is worse than a missing one under flash-sale load.
func New(addr string, log *slog.Logger, opts ...Option) *Store {
	cfg := settings{
		failureThreshold: defaultFailureThreshold,
		cooldown:         defaultCooldown,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if addr == "" {
		return &Store{log: log}
	}
	return &Store{
		client: redis.NewClient(&redis.Options{
			Addr:            addr,
			DialTimeout:     500 * time.Millisecond,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			PoolTimeout:     time.Second,
			MaxRetries:      1,
			MinRetryBackoff: 100 * time.Millisecond,
			MaxRetryBackoff: 300 * time.Millisecond,
		}),
		log: log,
		br:  newBreaker(cfg.failureThreshold, cfg.cooldown),
	}
}

// Set records hold:{reservationID} with the remaining retention window
// as TTL. Redis keys expire on their own, so the sweeper only needs to
// clean them up as hygiene.
func (s *Store) Set(ctx context.Context, reservationID string, ttl time.Duration) error {
	if s.client == nil || ttl <= 0 {
		return nil
	}
	if !s.br.allow() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := s.client.Set(ctx, Key(reservationID), "1", ttl).Err(); err != nil {
		s.degraded("set", reservationID, err)
		return nil
	}
	s.recovered()
	return nil
}

// Release deletes hold:{reservationID} after the reservation reaches a
// terminal state. Unknown keys are not an error.
func (s *Store) Release(ctx context.Context, reservationID string) error {
	if s.client == nil {
		return nil
	}
	if !s.br.allow() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := s.client.Del(ctx, Key(reservationID)).Err(); err != nil {
		s.degraded("release", reservationID, err)
		return nil
	}
	s.recovered()
	return nil
}

// Exists reports whether the hold key is present. Redis failures report
// false; callers must treat it as a hint, never as authority.
func (s *Store) Exists(ctx context.Context, reservationID string) bool {
	if s.client == nil {
		return false
	}
	if !s.br.allow() {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	n, err := s.client.Exists(ctx, Key(reservationID)).Result()
	if err != nil {
		s.degraded("exists", reservationID, err)
		return false
	}
	s.recovered()
	return n > 0
}

// Close releases the client connection pool.
func (s *Store) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

// degraded records a failed operation: the per-op warning stays bounded
// by the breaker threshold, and the open transition is announced once.
func (s *Store) degraded(op, reservationID string, err error) {
	if opened := s.br.failure(); opened {
		s.log.Warn("redis hold breaker open; fast-path short-circuited until cooldown elapses",
			"cooldown", s.br.cooldown,
		)
	}
	s.log.Warn("redis hold degraded; continuing without fast-path",
		"op", op,
		"reservation_id", reservationID,
		"error", err,
	)
}

// recovered closes the breaker after a successful operation, with a
// single log when the store had been degraded.
func (s *Store) recovered() {
	if s.br.success() {
		s.log.Info("redis hold recovered; fast-path re-enabled")
	}
}

// breaker counts consecutive failures and, past the threshold,
// short-circuits operations for the cooldown window. The first
// operation after the cooldown is the half-open probe: it may reach
// Redis alone, and its outcome either closes the breaker or re-arms
// the cooldown. All methods are safe for concurrent use.
type breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	failures  int
	openedAt  time.Time
}

func newBreaker(threshold int, cooldown time.Duration) *breaker {
	return &breaker{threshold: threshold, cooldown: cooldown}
}

// allow reports whether an operation may reach Redis. While open it
// returns false; when the cooldown has elapsed it re-arms the window
// and lets exactly one operation through as the probe.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return true
	}
	if time.Since(b.openedAt) < b.cooldown {
		return false
	}
	b.openedAt = time.Now()
	return true
}

// success resets the count after a good operation and reports whether
// the store had been degraded.
func (b *breaker) success() (wasOpen bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	wasOpen = b.failures >= b.threshold
	b.failures = 0
	return wasOpen
}

// failure records a failed operation and reports whether it opened the
// breaker. A failed probe (more failures than the threshold) re-arms
// the cooldown so the next attempt is again a single probe.
func (b *breaker) failure() (opened bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures >= b.threshold {
		b.openedAt = time.Now()
		return b.failures == b.threshold
	}
	return false
}
