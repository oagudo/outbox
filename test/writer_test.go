package test

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/oagudo/go-outbox/pkg/outbox"
	"github.com/stretchr/testify/require"
)

var (
	db *sql.DB
)

type entity struct {
	ID        uuid.UUID
	CreatedAt time.Time
}

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

	anyMsg := createMessageFixture()
	anyEntity := createEntityFixture()

	err := w.Write(context.Background(), anyMsg, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO Entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
		require.NoError(t, err)
		return nil
	})

	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM Outbox").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	var savedMessage outbox.Message
	var savedEntity entity

	err = db.QueryRow("SELECT id, created_at, context, payload FROM Outbox WHERE id = $1", anyMsg.ID).Scan(
		&savedMessage.ID, &savedMessage.CreatedAt, &savedMessage.Context, &savedMessage.Payload,
	)
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, created_at FROM Entity WHERE id = $1", anyEntity.ID).Scan(
		&savedEntity.ID, &savedEntity.CreatedAt,
	)
	require.NoError(t, err)

	assertEntityEqual(t, anyEntity, savedEntity)
	assertMessageEqual(t, anyMsg, savedMessage)
}

type failingTxProvider struct {
	tx *sql.Tx
}

func (f *failingTxProvider) Begin() (*sql.Tx, error) {
	return f.tx, errors.New("any error in callback")
}

func TestWriterRollsBackOnCallbackError(t *testing.T) {
	w := outbox.NewWriter(db)

	anyMsg := createMessageFixture()
	anyEntity := createEntityFixture()

	err := w.Write(context.Background(), anyMsg, func(ctx context.Context, tx *sql.Tx) error {
		return nil
	})
	require.NoError(t, err)

	err = w.Write(context.Background(), anyMsg, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO Entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.Error(t, err) // Uniqueness constraint violation when storing the outbox message
	var pqError *pq.Error
	require.ErrorAs(t, err, &pqError)
	require.Equal(t, pq.ErrorCode("23505"), pqError.Code)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM Entity WHERE id = $1", anyEntity.ID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestWriterRollsBackOnOutboxWriteError(t *testing.T) {
	w := outbox.NewWriter(db)

	anyMsg := createMessageFixture()

	err := w.Write(context.Background(), anyMsg, func(ctx context.Context, tx *sql.Tx) error {
		return errors.New("any error in callback")
	})

	require.Error(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM Outbox").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
func createMessageFixture() outbox.Message {
	msgID := uuid.New()
	msgContext := []byte("{}")
	msgPayload := []byte("{}")
	createdAt := time.Now().UTC().Truncate(time.Microsecond)

	return outbox.Message{
		ID:        msgID,
		CreatedAt: createdAt,
		Context:   msgContext,
		Payload:   msgPayload,
	}
}

func createEntityFixture() entity {
	entityID := uuid.New()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)

	return entity{
		ID:        entityID,
		CreatedAt: createdAt,
	}
}

func assertMessageEqual(t *testing.T, expected, actual outbox.Message) {
	require.Equal(t, expected.ID, actual.ID)
	require.True(t, expected.CreatedAt.Equal(actual.CreatedAt))
	require.Equal(t, expected.Context, actual.Context)
	require.Equal(t, expected.Payload, actual.Payload)
}

func assertEntityEqual(t *testing.T, expected, actual entity) {
	require.Equal(t, expected.ID, actual.ID)
	require.True(t, expected.CreatedAt.Equal(actual.CreatedAt))
}
