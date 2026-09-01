// Package holds implements the Redis fast-path accelerator for
// reservation holds (hold:{reservation_id}). PostgreSQL remains the
// authority for the inventory: a missing or failing hold only costs a
// fast-path miss, so every Redis problem degrades to a warning log
// instead of failing the reservation flow.
package holds

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix namespaces hold keys on the shared Redis instance.
const KeyPrefix = "hold:"

// opTimeout caps every Redis round trip. The hold is an accelerator:
// a slow or unavailable Redis must cost at most this much per
// operation, never the flow itself.
const opTimeout = 200 * time.Millisecond

// Key returns the Redis key that accelerates a reservation.
func Key(reservationID string) string {
	return KeyPrefix + reservationID
}

// Store is the Redis-backed hold accelerator. A zero client (empty
// address) disables the store: every method is a no-op.
type Store struct {
	client *redis.Client
	log    *slog.Logger
}

// New wires the hold store against addr. An empty addr disables the
// accelerator entirely; clients get aggressive timeouts because a slow
// accelerator is worse than a missing one under flash-sale load.
func New(addr string, log *slog.Logger) *Store {
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
	}
}

// Set records hold:{reservationID} with the remaining retention window
// as TTL. Redis keys expire on their own, so the sweeper only needs to
// clean them up as hygiene.
func (s *Store) Set(ctx context.Context, reservationID string, ttl time.Duration) error {
	if s.client == nil || ttl <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := s.client.Set(ctx, Key(reservationID), "1", ttl).Err(); err != nil {
		s.degraded("set", reservationID, err)
	}
	return nil
}

// Release deletes hold:{reservationID} after the reservation reaches a
// terminal state. Unknown keys are not an error.
func (s *Store) Release(ctx context.Context, reservationID string) error {
	if s.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := s.client.Del(ctx, Key(reservationID)).Err(); err != nil {
		s.degraded("release", reservationID, err)
	}
	return nil
}

// Exists reports whether the hold key is present. Redis failures report
// false; callers must treat it as a hint, never as authority.
func (s *Store) Exists(ctx context.Context, reservationID string) bool {
	if s.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	n, err := s.client.Exists(ctx, Key(reservationID)).Result()
	if err != nil {
		s.degraded("exists", reservationID, err)
		return false
	}
	return n > 0
}

// Close releases the client connection pool.
func (s *Store) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *Store) degraded(op, reservationID string, err error) {
	s.log.Warn("redis hold degraded; continuing without fast-path",
		"op", op,
		"reservation_id", reservationID,
		"error", err,
	)
}
