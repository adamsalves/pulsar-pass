// Package logger provides the standard structured logger used by all
// PulsarPass services.
package logger

import (
	"log/slog"
	"os"
)

// New returns a slog.Logger configured for the given environment:
// JSON in production, human-friendly text otherwise.
func New(env string) *slog.Logger {
	if env == "production" {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}
