package outbox

import (
	"time"

	"github.com/google/uuid"
)

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
}
