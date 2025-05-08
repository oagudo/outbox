package outbox

import (
	"time"

	"github.com/google/uuid"
)

type Message struct {
	ID        uuid.UUID
	CreatedAt time.Time
	Context   []byte
	Payload   []byte
}
