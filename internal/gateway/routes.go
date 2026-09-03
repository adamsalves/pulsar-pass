package gateway

import (
	"log/slog"
	"net/http"
)

// Routes assembles the public HTTP surface with the middleware chain.
// Auth sits innermost — identity is a precondition for every handler —
// and inside the observability layer, so 401s are counted, traced and
// logged like any other status. Metrics wrap the whole chain so route,
// status and duration are recorded even when a downstream middleware
// short-circuits the response; Tracing sits inside RequestID so the
// server span carries the assigned request id.
func Routes(h *ReservationHandler, log *slog.Logger, tokens map[string]string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/reservations", http.HandlerFunc(h.Create))
	mux.Handle("GET /v1/reservations/{id}", http.HandlerFunc(h.Get))
	mux.Handle("POST /v1/reservations/{id}/payment", http.HandlerFunc(h.ConfirmPayment))

	return Metrics(RequestID(Tracing(Logging(log)(Recover(log)(Auth(tokens)(mux))))))
}
