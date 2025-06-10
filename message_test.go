package outbox

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMessageOptions(t *testing.T) {
	customID := uuid.New()
	customTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	msg := NewMessage(
		[]byte("payload"),
		WithID(customID),
		WithCreatedAt(customTime),
		WithScheduledAt(customTime),
		WithMetadata([]byte("metadata")),
	)

	if msg.ID != customID {
		t.Errorf("expected ID to be %v, got %v", customID, msg.ID)
	}
	if !msg.CreatedAt.Equal(customTime) {
		t.Errorf("expected CreatedAt to be %v, got %v", customTime, msg.CreatedAt)
	}
	if !msg.ScheduledAt.Equal(customTime) {
		t.Errorf("expected ScheduledAt to be %v, got %v", customTime, msg.ScheduledAt)
	}
	if !bytes.Equal(msg.Payload, []byte("payload")) {
		t.Errorf("expected Payload to be %v, got %v", []byte("payload"), msg.Payload)
	}
	if !bytes.Equal(msg.Metadata, []byte("metadata")) {
		t.Errorf("expected Metadata to be %v, got %v", []byte("metadata"), msg.Metadata)
	}
	if msg.TimesAttempted != 0 {
		t.Errorf("expected TimesAttempted to be 0, got %v", msg.TimesAttempted)
	}
}
