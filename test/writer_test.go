package test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/oagudo/outbox/pkg/outbox"
	"github.com/stretchr/testify/require"
)

type entity struct {
	ID        uuid.UUID
	CreatedAt time.Time
}

func TestWriterSuccessfullyWritesToOutbox(t *testing.T) {
	w := outbox.NewWriter(db)

	anyMsg := createMessageFixture()
	anyEntity := createEntityFixture()

	err := w.Write(context.Background(), anyMsg, func(ctx context.Context, executor outbox.QueryExecutor) error {
		err := executor.ExecContext(ctx, "INSERT INTO Entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	savedMessage, found := readOutboxMessage(t, anyMsg.ID)
	require.True(t, found)

	savedEntity, found := readEntity(t, anyEntity.ID)
	require.True(t, found)

	assertEntityEqual(t, anyEntity, *savedEntity)
	assertMessageEqual(t, anyMsg, *savedMessage)
}

func TestWriterRollsBackOnOutboxWriteError(t *testing.T) {
	w := outbox.NewWriter(db)

	// Write a message to the outbox
	anyMsg := createMessageFixture()
	err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.QueryExecutor) error {
		return nil
	})
	require.NoError(t, err)

	anyEntity := createEntityFixture()
	err = w.Write(context.Background(), anyMsg, func(ctx context.Context, executor outbox.QueryExecutor) error {
		err := executor.ExecContext(ctx, "INSERT INTO Entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.Error(t, err) // Uniqueness constraint violation when storing the outbox message
	var pqError *pq.Error
	require.ErrorAs(t, err, &pqError)
	require.Equal(t, pq.ErrorCode("23505"), pqError.Code)

	_, found := readEntity(t, anyEntity.ID)
	require.False(t, found)
}

func TestWriterRollsBackOnCallbackError(t *testing.T) {
	w := outbox.NewWriter(db)

	anyMsg := createMessageFixture()

	err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.QueryExecutor) error {
		return errors.New("any error in callback")
	})

	require.Error(t, err)

	_, found := readOutboxMessage(t, anyMsg.ID)
	require.False(t, found)
}

type fakePublisher struct {
	publishErr error
	published  bool
	onPublish  func(outbox.Message)
}

func (p *fakePublisher) Publish(_ context.Context, msg outbox.Message) error {
	p.published = true
	if p.onPublish != nil {
		p.onPublish(msg)
	}
	return p.publishErr
}

func TestWriterWithOptimisticPublisher(t *testing.T) {
	t.Run("publishes message and removes it from outbox if callback succeeds", func(t *testing.T) {
		publisher := &fakePublisher{}
		w := outbox.NewWriter(db, outbox.WithOptimisticPublisher(publisher))

		anyMsg := createMessageFixture()
		err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.QueryExecutor) error {
			return nil
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, found := readOutboxMessage(t, anyMsg.ID)
			return publisher.published && !found
		}, time.Second, 50*time.Millisecond)
	})

	t.Run("does remove message from outbox if publisher returns an error", func(t *testing.T) {
		publisher := &fakePublisher{publishErr: errors.New("any publisher error")}
		w := outbox.NewWriter(db, outbox.WithOptimisticPublisher(publisher))

		anyMsg := createMessageFixture()
		err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.QueryExecutor) error {
			return nil
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return publisher.published
		}, time.Second, 50*time.Millisecond)

		_, found := readOutboxMessage(t, anyMsg.ID)
		require.True(t, found)
	})
}

func readOutboxMessage(t *testing.T, id uuid.UUID) (*outbox.Message, bool) {
	t.Helper()

	var msg outbox.Message
	err := db.QueryRow("SELECT id, created_at, context, payload FROM Outbox WHERE id = $1", id).Scan(
		&msg.ID, &msg.CreatedAt, &msg.Context, &msg.Payload,
	)
	if err == sql.ErrNoRows {
		return nil, false
	}
	require.NoError(t, err)
	return &msg, true
}

func readEntity(t *testing.T, id uuid.UUID) (*entity, bool) {
	t.Helper()

	var e entity
	err := db.QueryRow("SELECT id, created_at FROM Entity WHERE id = $1", id).Scan(
		&e.ID, &e.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, false
	}
	require.NoError(t, err)
	return &e, true
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
	t.Helper()

	require.Equal(t, expected.ID, actual.ID)
	require.True(t, expected.CreatedAt.Equal(actual.CreatedAt))
	require.Equal(t, expected.Context, actual.Context)
	require.Equal(t, expected.Payload, actual.Payload)
}

func assertEntityEqual(t *testing.T, expected, actual entity) {
	t.Helper()

	require.Equal(t, expected.ID, actual.ID)
	require.True(t, expected.CreatedAt.Equal(actual.CreatedAt))
}
