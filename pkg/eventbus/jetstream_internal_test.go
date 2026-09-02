package eventbus

import (
	"testing"
	"time"
)

func TestRedeliveryDelayPacesAttempts(t *testing.T) {
	cases := []struct {
		delivered uint64
		want      time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 250 * time.Millisecond},
		{3, 500 * time.Millisecond},
		{4, time.Second},
		{5, 2 * time.Second},
		{10, 2 * time.Second},
		{1000, 2 * time.Second},
	}
	for _, tc := range cases {
		if got := redeliveryDelay(tc.delivered); got != tc.want {
			t.Errorf("redeliveryDelay(%d) = %v, want %v", tc.delivered, got, tc.want)
		}
	}
}
