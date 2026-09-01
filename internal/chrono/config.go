// Package chrono is the TTL and expiration worker. It sweeps the
// reservations table for overdue pending reservations and emits the
// compensation events that release the seats back to the pool.
package chrono

import (
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/config"
)

// Config carries the pulsar-chrono runtime settings.
type Config struct {
	Env           string
	HealthAddr    string
	DatabaseURL   string
	NATSURL       string
	RedisAddr     string
	SweepInterval time.Duration
	SweepBatch    int
}

// LoadConfig reads pulsar-chrono settings from the environment.
func LoadConfig() Config {
	return Config{
		Env:         config.String("APP_ENV", "development"),
		HealthAddr:  config.String("HEALTH_ADDR", ":9093"),
		DatabaseURL: config.String("DATABASE_URL", "postgres://pulsar:pulsar@localhost:5432/pulsar_core?sslmode=disable"),
		NATSURL:     config.String("NATS_URL", "nats://localhost:4222"),
		// Empty REDIS_ADDR explicitly disables hold cleanup; only an
		// unset variable falls back to the default.
		RedisAddr:     config.StringAllowEmpty("REDIS_ADDR", "localhost:6379"),
		SweepInterval: config.Duration("SWEEP_INTERVAL", 5*time.Second),
		SweepBatch:    config.Int("SWEEP_BATCH", 100),
	}
}
