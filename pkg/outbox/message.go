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

	// ScheduledAt is the timestamp when the message should be published
	ScheduledAt time.Time

	// Metadata is an optional field containing additional information about the message,
	// such as correlation IDs, trace IDs, user context, or other custom attributes.
	// This data is typically JSON-serialized and can be used for tracing, debugging, or routing purposes.
	// Most message brokers support attaching such metadata as message headers, enabling richer message processing and observability.
	Metadata []byte

	// Payload contains the actual message data, typically JSON serialized
	Payload []byte

	// TimesAttempted is the number of times the message has been attempted to be published
	// Read only field
	TimesAttempted int32
}

// WithID sets the unique identifier of the message.
// If not provided, a new UUID will be generated.
func WithID(id uuid.UUID) func(*Message) {
	return func(m *Message) {
		m.ID = id
	}
}

// WithCreatedAt sets the time the message was created.
// If not provided, the current time will be used.
func WithCreatedAt(createdAt time.Time) func(*Message) {
	return func(m *Message) {
		m.CreatedAt = createdAt
	}
}

// WithScheduledAt sets the time the message should be published.
// If not provided, the current time will be used.
func WithScheduledAt(scheduledAt time.Time) func(*Message) {
	return func(m *Message) {
		m.ScheduledAt = scheduledAt
	}
}

// WithMetadata attaches message metadata (e.g. correlation ID, trace ID, etc).
func WithMetadata(metadata []byte) MessageOption {
	return func(m *Message) {
		m.Metadata = metadata
	}
}

// NewMessage creates a new Message with the given payload.
func NewMessage(payload []byte, opts ...MessageOption) *Message {
	now := time.Now().UTC()

	m := &Message{
		ID:             uuid.New(),
		CreatedAt:      now,
		ScheduledAt:    now,
		Payload:        payload,
		TimesAttempted: 0,
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}
