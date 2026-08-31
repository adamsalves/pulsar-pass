// Package payment is the payment processor service. It consumes payment
// commands, charges the acquirer (simulated in the MVP) and emits
// payment.succeeded or payment.failed events.
package payment

import (
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/config"
)

// Config carries the pulsar-payment runtime settings.
type Config struct {
	Env                  string
	HealthAddr           string
	DatabaseURL          string
	NATSURL              string
	ChargeDelay          time.Duration
	SimulatedFailureRate float64
}

// LoadConfig reads pulsar-payment settings from the environment.
func LoadConfig() Config {
	return Config{
		Env:                  config.String("APP_ENV", "development"),
		HealthAddr:           config.String("HEALTH_ADDR", ":9094"),
		DatabaseURL:          config.String("DATABASE_URL", "postgres://pulsar:pulsar@localhost:5432/pulsar_payment?sslmode=disable"),
		NATSURL:              config.String("NATS_URL", "nats://localhost:4222"),
		ChargeDelay:          config.Duration("SIMULATED_CHARGE_DELAY", 250*time.Millisecond),
		SimulatedFailureRate: config.Float("SIMULATED_FAILURE_RATE", 0.05),
	}
}
