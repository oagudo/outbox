package test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/oagudo/outbox"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)
	t.Run("stores message and executes queries atomically", func(t *testing.T) {
		anyMsg := createMessageFixture()
		anyEntity := createEntityFixture()

		err := w.Write(context.Background(), func(ctx context.Context, tx outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
			if err != nil {
				return err
			}
			return msgWriter.Store(ctx, anyMsg)
		})
		require.NoError(t, err)

		savedMessage, found := readOutboxMessage(t, anyMsg.ID)
		require.True(t, found)
		assertMessageEqual(t, anyMsg, savedMessage)

		savedEntity, found := readEntity(t, anyEntity.ID)
		require.True(t, found)
		assertEntityEqual(t, anyEntity, *savedEntity)
	})

	t.Run("stores multiple messages in single transaction", func(t *testing.T) {
		firstMsg := createMessageFixture()
		secondMsg := createMessageFixture()
		anyEntity := createEntityFixture()

		err := w.Write(context.Background(), func(ctx context.Context, tx outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
			if err != nil {
				return err
			}
			err = msgWriter.Store(ctx, firstMsg)
			if err != nil {
				return err
			}
			return msgWriter.Store(ctx, secondMsg)
		})
		require.NoError(t, err)

		requireMessageInOutbox(t, firstMsg.ID)
		requireMessageInOutbox(t, secondMsg.ID)

		requireEntityInDB(t, anyEntity.ID)
	})

	t.Run("commits transaction with zero messages", func(t *testing.T) {
		anyEntity := createEntityFixture()

		err := w.Write(context.Background(), func(ctx context.Context, tx outbox.TxQueryer, _ outbox.MessageWriter) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
			return err
		})
		require.NoError(t, err)

		savedEntity, found := readEntity(t, anyEntity.ID)
		require.True(t, found)
		assertEntityEqual(t, anyEntity, *savedEntity)
	})

	t.Run("conditionally stores message based on query result", func(t *testing.T) {
		existingEntity := createEntityFixture()
		_, err := db.Exec("INSERT INTO entity (id, created_at) VALUES ($1, $2)", existingEntity.ID, existingEntity.CreatedAt)
		require.NoError(t, err)

		anyMsg := createMessageFixture()

		// Should not store message because entity already exists
		err = w.Write(context.Background(), func(ctx context.Context, tx outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			var id uuid.UUID
			err := tx.QueryRowContext(ctx, "SELECT id FROM entity WHERE id = $1", existingEntity.ID).Scan(&id)
			if errors.Is(err, sql.ErrNoRows) {
				// Only store message if entity doesn't exist
				return msgWriter.Store(ctx, anyMsg)
			}
			return err
		})
		require.NoError(t, err)

		requireMessageNotInOutbox(t, anyMsg.ID)
	})

	t.Run("rolls back on callback error", func(t *testing.T) {
		anyMsg := createMessageFixture()
		anyEntity := createEntityFixture()

		err := w.Write(context.Background(), func(ctx context.Context, tx outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
			if err != nil {
				return err
			}
			err = msgWriter.Store(ctx, anyMsg)
			if err != nil {
				return err
			}
			return errors.New("any error in callback")
		})
		require.Error(t, err)

		requireMessageNotInOutbox(t, anyMsg.ID)

		requireEntityNotInDB(t, anyEntity.ID)
	})

	t.Run("rolls back on message store error", func(t *testing.T) {
		anyMsg := createMessageFixture()
		// Pre-insert message to cause uniqueness violation
		err := w.Write(context.Background(), func(ctx context.Context, _ outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			return msgWriter.Store(ctx, anyMsg)
		})
		require.NoError(t, err)

		anyEntity := createEntityFixture()
		err = w.Write(context.Background(), func(ctx context.Context, tx outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
			if err != nil {
				return err
			}
			return msgWriter.Store(ctx, anyMsg) // duplicate message ID
		})
		require.Error(t, err)

		var pqError *pq.Error
		require.ErrorAs(t, err, &pqError)
		require.Equal(t, pq.ErrorCode("23505"), pqError.Code)

		requireEntityNotInDB(t, anyEntity.ID)
	})

	t.Run("returns error on transaction begin failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // forces error on tx begin

		err := w.Write(ctx, func(_ context.Context, _ outbox.TxQueryer, _ outbox.MessageWriter) error {
			return nil
		})
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestWriteWithOptimisticPublisher(t *testing.T) {
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)

	t.Run("publishes messages and removes them from outbox on success", func(t *testing.T) {
		var publishedCount atomic.Int32
		publisher := &fakePublisher{
			onPublish: func(_ context.Context, _ *outbox.Message) error {
				publishedCount.Add(1)
				return nil
			},
		}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		firstMsg := createMessageFixture()
		secondMsg := createMessageFixture()

		err := w.Write(context.Background(), func(ctx context.Context, _ outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			err := msgWriter.Store(ctx, firstMsg)
			if err != nil {
				return err
			}
			return msgWriter.Store(ctx, secondMsg)
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return publishedCount.Load() == 2
		}, testTimeout, pollInterval)

		requireEventuallyMessageNotInOutbox(t, firstMsg.ID)
		requireEventuallyMessageNotInOutbox(t, secondMsg.ID)
	})

	t.Run("does not remove message from outbox if publisher returns an error", func(t *testing.T) {
		publisher := &fakePublisher{
			onPublish: func(_ context.Context, _ *outbox.Message) error {
				return errors.New("any publisher error")
			},
		}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		anyMsg := createMessageFixture()
		err := w.Write(context.Background(), func(ctx context.Context, _ outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			return msgWriter.Store(ctx, anyMsg)
		})
		require.NoError(t, err)

		require.Eventually(t, publisher.publishCalled.Load, testTimeout, pollInterval)
		requireEventuallyMessageInOutbox(t, anyMsg.ID)
	})

	t.Run("does not remove message from outbox if optimistic publishing takes too long", func(t *testing.T) {
		publisher := &fakePublisher{}
		w := outbox.NewWriter(dbCtx,
			outbox.WithOptimisticPublisher(publisher),
			outbox.WithOptimisticTimeout(0), // context should be cancelled
		)

		anyMsg := createMessageFixture()
		err := w.Write(context.Background(), func(ctx context.Context, _ outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			return msgWriter.Store(ctx, anyMsg)
		})
		require.NoError(t, err)

		require.Eventually(t, publisher.publishCalled.Load, testTimeout, pollInterval)
		requireEventuallyMessageInOutbox(t, anyMsg.ID)
	})

	t.Run("publishes messages in CreatedAt order regardless of store order", func(t *testing.T) {
		var publishOrder []uuid.UUID
		var mu sync.Mutex
		publisher := &fakePublisher{
			onPublish: func(_ context.Context, msg *outbox.Message) error {
				mu.Lock()
				defer mu.Unlock()
				publishOrder = append(publishOrder, msg.ID)
				return nil
			},
		}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		now := time.Now().UTC()
		oldestMsg := createMessageFixture(outbox.WithCreatedAt(now.Add(-2 * time.Second)))
		middleMsg := createMessageFixture(outbox.WithCreatedAt(now.Add(-1 * time.Second)))
		newestMsg := createMessageFixture(outbox.WithCreatedAt(now))

		// Store in reverse order (newest first)
		err := w.Write(context.Background(), func(ctx context.Context, _ outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			if err := msgWriter.Store(ctx, newestMsg); err != nil {
				return err
			}
			if err := msgWriter.Store(ctx, middleMsg); err != nil {
				return err
			}
			return msgWriter.Store(ctx, oldestMsg)
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(publishOrder) == 3
		}, testTimeout, pollInterval)

		// Verify messages were published in CreatedAt order (oldest first)
		require.Equal(t, oldestMsg.ID, publishOrder[0])
		require.Equal(t, middleMsg.ID, publishOrder[1])
		require.Equal(t, newestMsg.ID, publishOrder[2])
	})

	t.Run("stops publishing on first error and leaves remaining messages in outbox", func(t *testing.T) {
		firstMsg := createMessageFixture()
		secondMsg := createMessageFixture()
		thirdMsg := createMessageFixture()

		var publishedIDs []uuid.UUID
		var mu sync.Mutex
		publisher := &fakePublisher{
			onPublish: func(_ context.Context, msg *outbox.Message) error {
				mu.Lock()
				defer mu.Unlock()
				if msg.ID == secondMsg.ID {
					return errors.New("simulated publish error on message B")
				}
				publishedIDs = append(publishedIDs, msg.ID)
				return nil
			},
		}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		err := w.Write(context.Background(), func(ctx context.Context, _ outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			if err := msgWriter.Store(ctx, firstMsg); err != nil {
				return err
			}
			if err := msgWriter.Store(ctx, secondMsg); err != nil {
				return err
			}
			return msgWriter.Store(ctx, thirdMsg)
		})
		require.NoError(t, err)

		requireEventuallyMessageNotInOutbox(t, firstMsg.ID)

		mu.Lock()
		require.Equal(t, []uuid.UUID{firstMsg.ID}, publishedIDs)
		mu.Unlock()

		requireMessageInOutbox(t, secondMsg.ID)
		requireMessageInOutbox(t, thirdMsg.ID)
	})

	t.Run("publishes ready messages and skips scheduled ones", func(t *testing.T) {
		now := time.Now().UTC()
		futureTime := now.Add(1 * time.Hour)

		firstReadyMsg := createMessageFixture()
		scheduledMsg := createMessageFixture(outbox.WithScheduledAt(futureTime))
		secondReadyMsg := createMessageFixture()

		var publishedIDs []uuid.UUID
		var mu sync.Mutex
		publisher := &fakePublisher{
			onPublish: func(_ context.Context, msg *outbox.Message) error {
				mu.Lock()
				defer mu.Unlock()
				publishedIDs = append(publishedIDs, msg.ID)
				return nil
			},
		}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		err := w.Write(context.Background(), func(ctx context.Context, _ outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
			if err := msgWriter.Store(ctx, firstReadyMsg); err != nil {
				return err
			}
			if err := msgWriter.Store(ctx, scheduledMsg); err != nil {
				return err
			}
			return msgWriter.Store(ctx, secondReadyMsg)
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(publishedIDs) == 2
		}, testTimeout, pollInterval)

		mu.Lock()
		require.Contains(t, publishedIDs, firstReadyMsg.ID)
		require.Contains(t, publishedIDs, secondReadyMsg.ID)
		require.NotContains(t, publishedIDs, scheduledMsg.ID)
		mu.Unlock()

		requireMessageNotInOutbox(t, firstReadyMsg.ID)
		requireMessageNotInOutbox(t, secondReadyMsg.ID)
		requireMessageInOutbox(t, scheduledMsg.ID)
	})
}

func TestWriteOne(t *testing.T) {
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)

	t.Run("stores message and executes queries atomically", func(t *testing.T) {
		anyMsg := createMessageFixture()
		anyEntity := createEntityFixture()

		err := w.WriteOne(context.Background(), anyMsg, func(ctx context.Context, tx outbox.TxQueryer) error {
			rows, err := tx.QueryContext(ctx, "SELECT id, created_at FROM entity WHERE id = $1", anyEntity.ID)
			require.NoError(t, err)
			defer func() {
				_ = rows.Close()
			}()
			require.False(t, rows.Next())

			_, err = tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		savedMessage, found := readOutboxMessage(t, anyMsg.ID)
		require.True(t, found)
		assertMessageEqual(t, anyMsg, savedMessage)

		savedEntity, found := readEntity(t, anyEntity.ID)
		require.True(t, found)
		assertEntityEqual(t, anyEntity, *savedEntity)
	})

	t.Run("returns error on transaction begin failure", func(t *testing.T) {
		anyMsg := createMessageFixture()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // forces error on tx begin

		err := w.WriteOne(ctx, anyMsg, func(_ context.Context, _ outbox.TxQueryer) error {
			return nil
		})
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)

		requireMessageNotInOutbox(t, anyMsg.ID)
	})

	t.Run("rolls back on message store error", func(t *testing.T) {
		anyMsg := createMessageFixture()
		err := w.WriteOne(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxQueryer) error {
			return nil
		})
		require.NoError(t, err)

		anyEntity := createEntityFixture()
		err = w.WriteOne(context.Background(), anyMsg, func(ctx context.Context, tx outbox.TxQueryer) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
			require.NoError(t, err)
			return nil
		})
		require.Error(t, err)

		var pqError *pq.Error
		require.ErrorAs(t, err, &pqError) // duplicate message ID
		require.Equal(t, pq.ErrorCode("23505"), pqError.Code)

		requireEntityNotInDB(t, anyEntity.ID)
	})

	t.Run("rolls back on callback error", func(t *testing.T) {
		anyMsg := createMessageFixture()

		err := w.WriteOne(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxQueryer) error {
			return errors.New("any error in callback")
		})

		require.Error(t, err)

		requireMessageNotInOutbox(t, anyMsg.ID)
	})
}

func TestWriteOneWithOptimisticPublisher(t *testing.T) {
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)

	t.Run("publishes message and removes it from outbox on success", func(t *testing.T) {
		publisher := &fakePublisher{}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		anyMsg := createMessageFixture()
		err := w.WriteOne(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxQueryer) error {
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
			onPublish: func(_ context.Context, _ *outbox.Message) error {
				return errors.New("any publisher error")
			},
		}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		anyMsg := createMessageFixture()
		err := w.WriteOne(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxQueryer) error {
			return nil
		})
		require.NoError(t, err)

		require.Eventually(t, publisher.publishCalled.Load, testTimeout, pollInterval)
		requireEventuallyMessageInOutbox(t, anyMsg.ID)
	})

	t.Run("does not remove message from outbox if optimistic publishing takes too long", func(t *testing.T) {
		publisher := &fakePublisher{}
		w := outbox.NewWriter(dbCtx,
			outbox.WithOptimisticPublisher(publisher),
			outbox.WithOptimisticTimeout(0),
		)

		anyMsg := createMessageFixture()
		err := w.WriteOne(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxQueryer) error {
			return nil
		})
		require.NoError(t, err)

		require.Eventually(t, publisher.publishCalled.Load, testTimeout, pollInterval)
		requireEventuallyMessageInOutbox(t, anyMsg.ID)
	})

	t.Run("does not publish message scheduled for the future", func(t *testing.T) {
		publisher := &fakePublisher{}
		w := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))

		futureTime := time.Now().UTC().Add(1 * time.Hour)
		scheduledMsg := createMessageFixture(outbox.WithScheduledAt(futureTime))

		err := w.WriteOne(context.Background(), scheduledMsg, func(_ context.Context, _ outbox.TxQueryer) error {
			return nil
		})
		require.NoError(t, err)

		require.Never(t, publisher.publishCalled.Load, 50*time.Millisecond, 10*time.Millisecond)
		requireMessageInOutbox(t, scheduledMsg.ID)
	})
}

func TestUnmanagedWriter(t *testing.T) {
	ctx := context.Background()

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)
	uw := w.Unmanaged()

	t.Run("stores messages on transaction commit", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback()
		}()

		firstMsg := createMessageFixture()
		secondMsg := createMessageFixture()

		err = uw.Store(ctx, tx, firstMsg)
		require.NoError(t, err)

		err = uw.Store(ctx, tx, secondMsg)
		require.NoError(t, err)

		anyEntity := createEntityFixture()
		_, err = tx.ExecContext(ctx, "INSERT INTO entity (id, created_at) VALUES ($1, $2)", anyEntity.ID, anyEntity.CreatedAt)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		firstSavedMessage, found := readOutboxMessage(t, firstMsg.ID)
		require.True(t, found)
		assertMessageEqual(t, firstMsg, firstSavedMessage)

		secondSavedMessage, found := readOutboxMessage(t, secondMsg.ID)
		require.True(t, found)
		assertMessageEqual(t, secondMsg, secondSavedMessage)

		savedEntity, found := readEntity(t, anyEntity.ID)
		require.True(t, found)
		assertEntityEqual(t, anyEntity, *savedEntity)
	})

	t.Run("does not store message without commit", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback()
		}()

		anyMsg := createMessageFixture()
		err = uw.Store(ctx, tx, anyMsg)
		require.NoError(t, err)

		requireMessageNotInOutbox(t, anyMsg.ID)
	})

	t.Run("returns error on duplicate message", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback()
		}()

		anyMsg := createMessageFixture()
		err = uw.Store(ctx, tx, anyMsg)
		require.NoError(t, err)

		err = uw.Store(ctx, tx, anyMsg) // duplicate message ID
		require.Error(t, err)

		var pqError *pq.Error
		require.ErrorAs(t, err, &pqError)
		require.Equal(t, pq.ErrorCode("23505"), pqError.Code)
	})

	t.Run("returns error on cancelled context", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback()
		}()

		ctx, cancel := context.WithCancel(ctx)
		cancel()

		anyMsg := createMessageFixture()
		err = uw.Store(ctx, tx, anyMsg)

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})
}

type entity struct {
	ID        uuid.UUID
	CreatedAt time.Time
}

type fakePublisher struct {
	publishCalled atomic.Bool
	onPublish     func(context.Context, *outbox.Message) error
}

func (p *fakePublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	p.publishCalled.Store(true)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.onPublish != nil {
		return p.onPublish(ctx, msg)
	}
	return nil
}

func readOutboxMessage(t *testing.T, id uuid.UUID) (*outbox.Message, bool) {
	t.Helper()

	var msg outbox.Message
	err := db.QueryRow("SELECT id, created_at, scheduled_at, metadata, payload, times_attempted FROM outbox WHERE id = $1", id).Scan(
		&msg.ID, &msg.CreatedAt, &msg.ScheduledAt, &msg.Metadata, &msg.Payload, &msg.TimesAttempted,
	)
	if err == sql.ErrNoRows {
		return nil, false
	}
	require.NoError(t, err)
	return &msg, true
}

func requireMessageInOutbox(t *testing.T, id uuid.UUID) {
	t.Helper()
	_, found := readOutboxMessage(t, id)
	require.True(t, found)
}

func requireMessageNotInOutbox(t *testing.T, id uuid.UUID) {
	t.Helper()
	_, found := readOutboxMessage(t, id)
	require.False(t, found)
}

func readEntity(t *testing.T, id uuid.UUID) (*entity, bool) {
	t.Helper()

	var e entity
	err := db.QueryRow("SELECT id, created_at FROM entity WHERE id = $1", id).Scan(
		&e.ID, &e.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, false
	}
	require.NoError(t, err)
	return &e, true
}

func requireEntityInDB(t *testing.T, id uuid.UUID) {
	t.Helper()
	_, found := readEntity(t, id)
	require.True(t, found)
}

func requireEntityNotInDB(t *testing.T, id uuid.UUID) {
	t.Helper()
	_, found := readEntity(t, id)
	require.False(t, found)
}

func requireEventuallyMessageInOutbox(t *testing.T, id uuid.UUID) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, id)
		return found
	}, testTimeout, pollInterval)
}

func requireEventuallyMessageNotInOutbox(t *testing.T, id uuid.UUID) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, id)
		return !found
	}, testTimeout, pollInterval)
}

func createMessageFixture(opts ...outbox.MessageOption) *outbox.Message {
	now := time.Now().UTC().Truncate(time.Millisecond)
	msgOpts := []outbox.MessageOption{
		outbox.WithCreatedAt(now),
		outbox.WithScheduledAt(now),
		outbox.WithMetadata([]byte(`{"any_metadata_key": "any_metadata_value"}`)),
	}
	msgOpts = append(msgOpts, opts...)
	msg := outbox.NewMessage(
		[]byte(`{"any_payload_key": "any_payload_value"}`),
		msgOpts...,
	)

	return msg
}

func createEntityFixture() entity {
	entityID := uuid.New()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)

	return entity{
		ID:        entityID,
		CreatedAt: createdAt,
	}
}

func assertMessageEqual(t *testing.T, expected, actual *outbox.Message) {
	t.Helper()

	require.Equal(t, expected.ID, actual.ID)
	require.True(t, expected.CreatedAt.Equal(actual.CreatedAt))
	require.Equal(t, expected.Metadata, actual.Metadata)
	require.Equal(t, expected.Payload, actual.Payload)
}

func assertEntityEqual(t *testing.T, expected, actual entity) {
	t.Helper()

	require.Equal(t, expected.ID, actual.ID)
	require.True(t, expected.CreatedAt.Equal(actual.CreatedAt))
}
