package test_test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/require"

	"go-outbox/pkg/outbox"
)

var (
	db *sql.DB
)

func TestMain(m *testing.M) {
	var err error
	db, err = sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/outbox?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %s", err)
	}

	log.Println("Connected to database")
	_, err = db.Exec("TRUNCATE TABLE Outbox")
	if err != nil {
		log.Fatalf("Failed to truncate outbox table: %s", err)
	}

	os.Exit(m.Run())
}

func TestWriterSuccessfullyWritesToOutbox(t *testing.T) {
	w := outbox.NewWriter(db)

	msgID := uuid.New()
	msgContext := []byte("{}")
	msgPayload := []byte("{}")
	createdAt := time.Now().UTC().Truncate(time.Microsecond)

	msg := outbox.Message{
		ID:        msgID,
		CreatedAt: createdAt,
		Context:   msgContext,
		Payload:   msgPayload,
	}

	err := w.Write(context.Background(), msg, func(ctx context.Context, tx *sql.Tx) error {
		return nil
	})

	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM Outbox").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Read the message from the outbox table and compare
	var savedID uuid.UUID
	var savedCreatedAt time.Time
	var savedContext []byte
	var savedPayload []byte

	err = db.QueryRow("SELECT id, created_at, context, payload FROM Outbox WHERE id = $1", msgID).Scan(
		&savedID, &savedCreatedAt, &savedContext, &savedPayload,
	)
	require.NoError(t, err)

	// Compare the saved message with the original
	require.Equal(t, msgID, savedID)
	require.True(t, createdAt.Equal(savedCreatedAt))
	require.Equal(t, msgContext, savedContext)
	require.Equal(t, msgPayload, savedPayload)
}
