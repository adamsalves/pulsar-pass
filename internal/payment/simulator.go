package payment

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/adamsalves/pulsar-pass/pkg/uid"
)

// SimulatedAcquirer fakes an external payment gateway for the MVP.
// Token "fail-me" forces a decline; other requests decline with
// probability FailureRate.
type SimulatedAcquirer struct {
	FailureRate float64
	Delay       time.Duration
}

// Charge simulates the remote charge, honouring context cancellation.
func (a *SimulatedAcquirer) Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error) {
	if a.Delay > 0 {
		select {
		case <-ctx.Done():
			return ChargeResult{}, ctx.Err()
		case <-time.After(a.Delay):
		}
	}
	if req.Token == "fail-me" {
		return ChargeResult{}, errors.New("card declined (forced by token)")
	}
	if rand.Float64() < a.FailureRate {
		return ChargeResult{}, errors.New("card declined")
	}
	return ChargeResult{GatewayRef: "sim-" + uid.New()}, nil
}
