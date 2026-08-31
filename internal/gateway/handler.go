package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/adamsalves/pulsar-pass/pkg/envelope"
	"github.com/adamsalves/pulsar-pass/pkg/eventbus"
	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// ReservationHandler exposes the reservation endpoints. Mutations are
// asynchronous: the handler validates the payload, enforces the
// Idempotency-Key contract and publishes a command to the broker.
type ReservationHandler struct {
	bus         eventbus.Publisher
	log         *slog.Logger
	maxQuantity int
}

// NewReservationHandler wires the reservation endpoints.
func NewReservationHandler(bus eventbus.Publisher, log *slog.Logger, maxQuantity int) *ReservationHandler {
	return &ReservationHandler{bus: bus, log: log, maxQuantity: maxQuantity}
}

type createReservationRequest struct {
	EventID  string `json:"event_id"`
	Quantity int    `json:"quantity"`
}

type reservationAcceptedResponse struct {
	Status        string `json:"status"`
	ReservationID string `json:"reservation_id"`
}

type confirmPaymentRequest struct {
	PaymentMethodToken string `json:"payment_method_token"`
}

// Create accepts a reservation intent and publishes it as a command.
func (h *ReservationHandler) Create(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	var req createReservationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.EventID == "" || req.Quantity <= 0 || req.Quantity > h.maxQuantity {
		writeError(w, http.StatusUnprocessableEntity,
			"event_id is required and quantity must be between 1 and "+strconv.Itoa(h.maxQuantity))
		return
	}
	if h.bus == nil {
		writeError(w, http.StatusServiceUnavailable, "booking temporarily unavailable")
		return
	}

	reservationID := uid.New()
	body, err := json.Marshal(map[string]any{
		"reservation_id": reservationID,
		"event_id":       req.EventID,
		"quantity":       req.Quantity,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	msg := eventbus.Message{
		ID:      uid.New(),
		Subject: envelope.SubjectReservationReserve,
		Payload: body,
		Headers: map[string]string{
			"X-Request-Id":    RequestIDFrom(r.Context()),
			"Idempotency-Key": idempotencyKey,
		},
	}
	if err := h.bus.Publish(r.Context(), msg); err != nil {
		h.log.Error("failed to publish reserve command",
			"request_id", RequestIDFrom(r.Context()),
			"error", err,
		)
		writeError(w, http.StatusServiceUnavailable, "booking temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusAccepted, reservationAcceptedResponse{
		Status:        "accepted",
		ReservationID: reservationID,
	})
}

// Get returns a reservation by id. The query flow is wired in the next
// cycle.
func (h *ReservationHandler) Get(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "query flow is wired in the next cycle")
}

// ConfirmPayment accepts the user payment submission inside the TTL
// window and publishes it as a command.
func (h *ReservationHandler) ConfirmPayment(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	var req confirmPaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.PaymentMethodToken == "" {
		writeError(w, http.StatusUnprocessableEntity, "payment_method_token is required")
		return
	}
	if h.bus == nil {
		writeError(w, http.StatusServiceUnavailable, "booking temporarily unavailable")
		return
	}

	reservationID := r.PathValue("id")
	body, err := json.Marshal(map[string]any{
		"reservation_id":       reservationID,
		"payment_method_token": req.PaymentMethodToken,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	msg := eventbus.Message{
		ID:      uid.New(),
		Subject: envelope.SubjectReservationPayment,
		Payload: body,
		Headers: map[string]string{
			"X-Request-Id":    RequestIDFrom(r.Context()),
			"Idempotency-Key": idempotencyKey,
		},
	}
	if err := h.bus.Publish(r.Context(), msg); err != nil {
		h.log.Error("failed to publish payment command",
			"request_id", RequestIDFrom(r.Context()),
			"reservation_id", reservationID,
			"error", err,
		)
		writeError(w, http.StatusServiceUnavailable, "booking temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusAccepted, reservationAcceptedResponse{
		Status:        "accepted",
		ReservationID: reservationID,
	})
}
