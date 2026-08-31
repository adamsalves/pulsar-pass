// Package core is the reservation and inventory service. It owns the
// stock of tickets, the reservation state machine and the authoritative
// inventory counters stored in PostgreSQL.
package core

import (
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/config"
)

// Config carries the pulsar-core runtime settings.
type Config struct {
	Env            string
	HealthAddr     string
	NATSURL        string
	RedisAddr      string
	DatabaseURL    string
	ReservationTTL time.Duration
}

// LoadConfig reads pulsar-core settings from the environment.
func LoadConfig() Config {
	return Config{
		Env:            config.String("APP_ENV", "development"),
		HealthAddr:     config.String("HEALTH_ADDR", ":9092"),
		NATSURL:        config.String("NATS_URL", "nats://localhost:4222"),
		RedisAddr:      config.String("REDIS_ADDR", "localhost:6379"),
		DatabaseURL:    config.String("DATABASE_URL", "postgres://pulsar:pulsar@localhost:5432/pulsar_core?sslmode=disable"),
		ReservationTTL: config.Duration("RESERVATION_TTL", 10*time.Minute),
	}
}
