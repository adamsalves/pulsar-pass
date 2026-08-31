package payment

import "context"

// PaymentRepository persists payment attempts.
type PaymentRepository interface {
	Create(ctx context.Context, p *Payment) error
	UpdateStatus(ctx context.Context, id string, status PaymentStatus, gatewayRef, failureReason string) error
}

// ChargeRequest carries the acquirer charge input.
type ChargeRequest struct {
	ReservationID  string
	UserID         string
	AmountCents    int64
	Currency       string
	Token          string
	IdempotencyKey string
}

// ChargeResult carries the acquirer charge output.
type ChargeResult struct {
	GatewayRef string
}

// Acquirer abstracts the external payment gateway.
type Acquirer interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
}
