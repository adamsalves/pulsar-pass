// Package config provides small helpers to load configuration from
// environment variables with sensible fallbacks.
package config

import (
	"os"
	"strconv"
	"time"
)

// String returns the environment variable named by key, or fallback when
// unset or empty.
func String(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Int returns the environment variable named by key parsed as int, or
// fallback when unset or unparsable.
func Int(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// Float returns the environment variable named by key parsed as float64,
// or fallback when unset or unparsable.
func Float(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

// Duration returns the environment variable named by key parsed as a
// time.Duration (e.g. "10m"), or fallback when unset or unparsable.
func Duration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
