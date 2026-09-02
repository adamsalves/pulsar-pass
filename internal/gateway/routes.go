package gateway

import (
	"log/slog"
	"net/http"
)

// Routes assembles the public HTTP surface with the middleware chain.
// Metrics wrap the whole chain: route, status and duration are recorded
// even when a downstream middleware short-circuits the response.
func Routes(h *ReservationHandler, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/reservations", http.HandlerFunc(h.Create))
	mux.Handle("GET /v1/reservations/{id}", http.HandlerFunc(h.Get))
	mux.Handle("POST /v1/reservations/{id}/payment", http.HandlerFunc(h.ConfirmPayment))

	return Tracing(Metrics(RequestID(Logging(log)(Recover(log)(mux)))))
}
