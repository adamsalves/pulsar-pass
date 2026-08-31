// Package pgpool provides a configured pgx connection pool factory
// shared by all PulsarPass services.
package pgpool

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Options tunes the pool.
type Options struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxConns == 0 {
		o.MaxConns = 10
	}
	if o.MinConns == 0 {
		o.MinConns = 1
	}
	if o.MaxConnLifetime == 0 {
		o.MaxConnLifetime = time.Hour
	}
	if o.MaxConnIdleTime == 0 {
		o.MaxConnIdleTime = 15 * time.Minute
	}
	return o
}

// New connects to url, pings it and returns the ready pool. A failed
// ping closes the pool before returning the error.
func New(ctx context.Context, url string, opts Options) (*pgxpool.Pool, error) {
	o := opts.withDefaults()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = o.MaxConns
	cfg.MinConns = o.MinConns
	cfg.MaxConnLifetime = o.MaxConnLifetime
	cfg.MaxConnIdleTime = o.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
