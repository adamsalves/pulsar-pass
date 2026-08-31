package gateway

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// Server serves the public HTTP API with hardened timeouts.
type Server struct {
	http *http.Server
}

// NewServer creates the API server bound to addr.
func NewServer(addr string, handler http.Handler) *Server {
	return &Server{http: &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}}
}

// ListenAndServe blocks serving requests until Shutdown is called. A
// closed server is not reported as an error.
func (s *Server) ListenAndServe() error {
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
