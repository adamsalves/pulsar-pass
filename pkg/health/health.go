// Package health provides a lightweight liveness/readiness HTTP server
// meant to run alongside each service.
package health

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Server exposes /healthz (liveness) and /readyz (readiness) endpoints.
type Server struct {
	log     *slog.Logger
	http    *http.Server
	ready   atomic.Bool
	version atomic.Pointer[string]
}

// NewServer creates a health server bound to addr.
func NewServer(addr string, log *slog.Logger) *Server {
	s := &Server{log: log}
	dev := "dev"
	s.version.Store(&dev)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Version", *s.version.Load())
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", s.handleReady)

	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// SetVersion records the build version reported by the X-Version header.
func (s *Server) SetVersion(v string) {
	s.version.Store(&v)
}

// SetReady toggles the readiness state reported by /readyz.
func (s *Server) SetReady(v bool) {
	s.ready.Store(v)
}

// ListenAndServe blocks serving the health endpoints until Shutdown is
// called. A closed server is not reported as an error.
func (s *Server) ListenAndServe() error {
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the health server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("X-Version", *s.version.Load())
	if !s.ready.Load() {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}
