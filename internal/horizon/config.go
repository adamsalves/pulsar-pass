// Package horizon is the outbox relay engine. It drains outbox_events
// tables and publishes them to the broker with at-least-once delivery
// backed by JetStream server-side deduplication.
package horizon

import (
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/config"
)

// Config carries the pulsar-horizon runtime settings.
type Config struct {
	Env          string
	HealthAddr   string
	CoreDBURL    string
	PaymentDBURL string
	NATSURL      string
	PollInterval time.Duration
	RelayBatch   int
}

// LoadConfig reads pulsar-horizon settings from the environment.
func LoadConfig() Config {
	return Config{
		Env:          config.String("APP_ENV", "development"),
		HealthAddr:   config.String("HEALTH_ADDR", ":9095"),
		CoreDBURL:    config.String("CORE_DATABASE_URL", "postgres://pulsar:pulsar@localhost:5432/pulsar_core?sslmode=disable"),
		PaymentDBURL: config.String("PAYMENT_DATABASE_URL", "postgres://pulsar:pulsar@localhost:5432/pulsar_payment?sslmode=disable"),
		NATSURL:      config.String("NATS_URL", "nats://localhost:4222"),
		PollInterval: config.Duration("POLL_INTERVAL", 1*time.Second),
		RelayBatch:   config.Int("RELAY_BATCH", 200),
	}
}
