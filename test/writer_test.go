package test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
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
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)

	anyMsg := createMessageFixture()
	anyEntity := createEntityFixture()

	err := w.Write(context.Background(), anyMsg, func(ctx context.Context, txExecFunc outbox.TxExecFunc) error {
		_, err := txExecFunc(ctx, "INSERT INTO Entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	savedMessage, found := readOutboxMessage(t, anyMsg.ID)
	require.True(t, found)
	assertMessageEqual(t, anyMsg, *savedMessage)

	savedEntity, found := readEntity(t, anyEntity.ID)
	require.True(t, found)
	assertEntityEqual(t, anyEntity, *savedEntity)
}

func TestWriterRollsBackOnOutboxMessageWriteError(t *testing.T) {
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)

	anyMsg := createMessageFixture()
	err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
		return nil
	})
	require.NoError(t, err)

	anyEntity := createEntityFixture()
	err = w.Write(context.Background(), anyMsg, func(ctx context.Context, txExecFunc outbox.TxExecFunc) error {
		_, err := txExecFunc(ctx, "INSERT INTO Entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
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

func TestWriterRollsBackOnUserDefinedCallbackError(t *testing.T) {
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)

	anyMsg := createMessageFixture()

	err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
		return errors.New("any error in callback")
	})

	require.Error(t, err)

	_, found := readOutboxMessage(t, anyMsg.ID)
	require.False(t, found)
}

type fakePublisher struct {
	publishCalled atomic.Bool
	onPublish     func(context.Context, outbox.Message) error
}

func (p *fakePublisher) Publish(ctx context.Context, msg outbox.Message) error {
	p.publishCalled.Store(true)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.onPublish != nil {
		return p.onPublish(ctx, msg)
	}
	return nil
}

func TestWriterWithOptimisticPublisher(t *testing.T) {
	t.Run("publishes message and removes it from outbox if callback succeeds", func(t *testing.T) {
		publisher := &fakePublisher{}
		dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		anyMsg := createMessageFixture()
		err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
			return nil
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, found := readOutboxMessage(t, anyMsg.ID)
			return publisher.publishCalled.Load() && !found
		}, testTimeout, pollInterval)
	})

	t.Run("does not remove message from outbox if publisher returns an error", func(t *testing.T) {
		publisher := &fakePublisher{
			onPublish: func(_ context.Context, _ outbox.Message) error {
				return errors.New("any publisher error")
			},
		}
		dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		anyMsg := createMessageFixture()
		err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
			return nil
		})
		require.NoError(t, err)

		require.Eventually(t, publisher.publishCalled.Load, testTimeout, pollInterval)

		_, found := readOutboxMessage(t, anyMsg.ID)
		require.True(t, found)
	})

	t.Run("does not remove message from outbox if optimistic publishing takes too long", func(t *testing.T) {
		publisher := &fakePublisher{}
		dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
		w := outbox.NewWriter(dbCtx,
			outbox.WithOptimisticPublisher(publisher),
			outbox.WithOptimisticTimeout(0), // context should be cancelled
		)

		anyMsg := createMessageFixture()
		err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
			return nil
		})
		require.NoError(t, err)

		require.Eventually(t, publisher.publishCalled.Load, testTimeout, pollInterval)

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
	msgContext := []byte(`{"any_context_key": "any_context_value"}`)
	msgPayload := []byte(`{"any_payload_key": "any_payload_value"}`)
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

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
