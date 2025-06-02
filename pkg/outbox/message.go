package outbox

import (
	"time"

	"github.com/google/uuid"
)

// MessageOption is a function that can be used to configure a Message.
type MessageOption func(*Message)

// Message represents a message to be published through the outbox pattern.
// It contains all the information needed to process and publish the message
// to an external system (like a message broker).
type Message struct {
	// ID is a unique identifier for the message
	ID uuid.UUID

	// CreatedAt is the timestamp when the message was created
	CreatedAt time.Time

	// Context is optional metadata about the message (correlation ID, trace ID, etc.), typically JSON serialized
	Context []byte

	// Payload contains the actual message data, typically JSON serialized
	Payload []byte

	// TimesAttempted is the number of times the message has been attempted to be published
	// Read only field
	TimesAttempted int32
}

// WithID sets the ID of the message.
// If not provided, a new UUID will be generated.
func WithID(id uuid.UUID) func(*Message) {
	return func(m *Message) {
		m.ID = id
	}
}

// WithCreatedAt sets the CreatedAt of the message.
// If not provided, the current time will be used.
func WithCreatedAt(createdAt time.Time) func(*Message) {
	return func(m *Message) {
		m.CreatedAt = createdAt
	}
}

// NewMessage creates a new Message with the given payload and context.
func NewMessage(payload []byte, ctx []byte, opts ...MessageOption) *Message {
	m := &Message{
		ID:             uuid.New(),
		CreatedAt:      time.Now().UTC(),
		Context:        ctx,
		Payload:        payload,
		TimesAttempted: 0,
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}
