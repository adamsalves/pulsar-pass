package payment

// PaymentStatus is the lifecycle of a payment.
type PaymentStatus string

const (
	PaymentStatusPending   PaymentStatus = "PENDING"
	PaymentStatusSucceeded PaymentStatus = "SUCCEEDED"
	PaymentStatusFailed    PaymentStatus = "FAILED"
)

// Payment is a charge attempt for a reservation.
type Payment struct {
	ID             string
	ReservationID  string
	UserID         string
	AmountCents    int64
	Currency       string
	Status         PaymentStatus
	IdempotencyKey string
	GatewayRef     string
	FailureReason  string
}
