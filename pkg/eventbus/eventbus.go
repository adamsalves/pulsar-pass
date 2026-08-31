// Package eventbus abstracts the message broker. Services depend on the
// Publisher/Subscriber interfaces; concrete implementations (NATS
// JetStream in production, in-memory for local development and tests)
// live here.
package eventbus

import (
	"context"
	"errors"
	"sync"
)

// Message is the unit of delivery on the bus.
type Message struct {
	// ID maps to the broker deduplication key (Nats-Msg-Id on JetStream).
	ID      string
	Subject string
	Payload []byte
	Headers map[string]string
}

// Handler processes a single message. Returning a non-nil error requests
// a redelivery (at-least-once semantics).
type Handler func(ctx context.Context, msg Message) error

// Publisher publishes messages to the bus.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}

// Subscriber registers handlers for subjects.
type Subscriber interface {
	Subscribe(subject string, queue string, handler Handler) error
}

// ErrNoHandler is returned when a message reaches a subject without
// registered handlers.
var ErrNoHandler = errors.New("eventbus: no handler for subject")

// Memory is an in-memory bus used for local development and tests. It is
// not durable and performs no deduplication.
type Memory struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

// NewMemory returns an empty in-memory bus.
func NewMemory() *Memory {
	return &Memory{handlers: make(map[string][]Handler)}
}

// Subscribe registers handler for subject under an optional queue group.
func (m *Memory) Subscribe(subject string, queue string, handler Handler) error {
	if subject == "" || handler == nil {
		return errors.New("eventbus: subject and handler are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[subject] = append(m.handlers[subject], handler)
	return nil
}

// Publish dispatches the message synchronously to all handlers registered
// for msg.Subject.
func (m *Memory) Publish(_ context.Context, msg Message) error {
	m.mu.RLock()
	handlers := m.handlers[msg.Subject]
	m.mu.RUnlock()

	if len(handlers) == 0 {
		return ErrNoHandler
	}
	for _, h := range handlers {
		if err := h(context.Background(), msg); err != nil {
			return err
		}
	}
	return nil
}

// Close satisfies the Bus interface; the in-memory bus holds no
// resources.
func (m *Memory) Close(context.Context) error {
	return nil
}
