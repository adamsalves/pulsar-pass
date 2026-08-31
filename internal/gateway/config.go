// Package gateway implements the HTTP ingress of PulsarPass. It
// validates requests, enforces idempotency headers and publishes
// commands to the broker.
package gateway

import (
	"github.com/adamsalves/pulsar-pass/pkg/config"
)

// Config carries the pulsar-gateway runtime settings.
type Config struct {
	Env         string
	HTTPAddr    string
	HealthAddr  string
	NATSURL     string
	RedisAddr   string
	MaxQuantity int
}

// LoadConfig reads pulsar-gateway settings from the environment.
func LoadConfig() Config {
	return Config{
		Env:         config.String("APP_ENV", "development"),
		HTTPAddr:    config.String("HTTP_ADDR", ":8080"),
		HealthAddr:  config.String("HEALTH_ADDR", ":9091"),
		NATSURL:     config.String("NATS_URL", "nats://localhost:4222"),
		RedisAddr:   config.String("REDIS_ADDR", "localhost:6379"),
		MaxQuantity: config.Int("MAX_RESERVATION_QTY", 8),
	}
}
