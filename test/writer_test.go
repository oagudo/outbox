package test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/oagudo/go-outbox/pkg/outbox"
	"github.com/stretchr/testify/require"
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

	entityID := uuid.New()

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
		_, err := tx.Exec("INSERT INTO Entity (id, created_at) VALUES ($1, $2)", entityID, createdAt)
		require.NoError(t, err)
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

	var savedEntityID uuid.UUID
	var savedEntityCreatedAt time.Time

	err = db.QueryRow("SELECT id, created_at, context, payload FROM Outbox WHERE id = $1", msgID).Scan(
		&savedID, &savedCreatedAt, &savedContext, &savedPayload,
	)
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, created_at FROM Entity WHERE id = $1", entityID).Scan(
		&savedEntityID, &savedEntityCreatedAt,
	)
	require.NoError(t, err)

	// Compare the saved message with the original
	require.Equal(t, entityID, savedEntityID)
	require.True(t, createdAt.Equal(savedEntityCreatedAt))

	require.Equal(t, msgID, savedID)
	require.True(t, createdAt.Equal(savedCreatedAt))
	require.Equal(t, msgContext, savedContext)
	require.Equal(t, msgPayload, savedPayload)
}
