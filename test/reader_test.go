package test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oagudo/outbox/pkg/outbox"
	"github.com/stretchr/testify/require"
)

const (
	readerInterval = 10 * time.Millisecond
	testTimeout    = 500 * time.Millisecond
	pollInterval   = 20 * time.Millisecond
)

func TestReaderSuccessfullyPublishesMessage(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg outbox.Message) {
			assertMessageEqual(t, anyMsg, msg)
		},
	}, outbox.WithInterval(readerInterval))
	r.Start()

	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, anyMsg.ID)
		return !found
	}, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestReaderPublishesMessagesInOrder(t *testing.T) {
	setupTest(t)

	firstMsg := createMessageFixture()

	secondMsg := createMessageFixture()
	secondMsg.CreatedAt = firstMsg.CreatedAt.Add(1 * time.Second)

	thirdMsg := createMessageFixture()
	thirdMsg.CreatedAt = firstMsg.CreatedAt.Add(2 * time.Second)

	msgs := []outbox.Message{
		firstMsg,
		secondMsg,
		thirdMsg,
	}

	writeMessages(t, msgs)

	var onPublishCalls int32 = 0
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg outbox.Message) {
			currentCalls := atomic.LoadInt32(&onPublishCalls)
			require.Equal(t, msg.ID, msgs[currentCalls].ID) // they are published in order
			atomic.AddInt32(&onPublishCalls, 1)
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithMaxMessages(1),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onPublishCalls) == int32(len(msgs)) //nolint:gosec
	}, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestReaderOnReadError(t *testing.T) {
	setupTest(t)

	t.Cleanup(func() {
		_, err := db.Exec("ALTER TABLE Outbox_old RENAME TO Outbox")
		require.NoError(t, err)
	})
	_, err := db.Exec("ALTER TABLE Outbox RENAME TO Outbox_old") // force an error on read
	require.NoError(t, err)

	var onReadCallbackCalled atomic.Bool
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{}, outbox.WithInterval(readerInterval),
		outbox.WithOnReadError(func(err error) {
			require.Error(t, err)
			onReadCallbackCalled.Store(true)
		}))
	r.Start()

	require.Eventually(t, onReadCallbackCalled.Load, testTimeout, pollInterval)

	err = r.Stop(context.Background())
	require.NoError(t, err)
}

func TestReaderOnDeleteError(t *testing.T) {
	setupTest(t)

	t.Cleanup(func() {
		_, err := db.Exec("ALTER TABLE Outbox_old RENAME TO Outbox")
		require.NoError(t, err)
	})

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var onDeleteCallbackCalled atomic.Bool
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ outbox.Message) {
			_, err := db.Exec("ALTER TABLE Outbox RENAME TO Outbox_old") // force an error on delete
			require.NoError(t, err)
		},
	}, outbox.WithInterval(readerInterval), outbox.WithOnDeleteError(func(_ outbox.Message, err error) {
		require.Error(t, err)
		onDeleteCallbackCalled.Store(true)
	}))
	r.Start()

	require.Eventually(t, onDeleteCallbackCalled.Load, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestStopTimesOutIfReaderIsNotStopped(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var wg sync.WaitGroup
	wg.Add(1)
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ outbox.Message) {
			wg.Done() // trigger for stop
			time.Sleep(100 * time.Millisecond)
		},
	}, outbox.WithInterval(readerInterval))
	r.Start()

	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := r.Stop(ctx)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestShouldTimeoutWhenReadingMessagesTakesTooLong(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var onReadErrorCalls int32 = 0
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithReadTimeout(0), // context should be cancelled
		outbox.WithOnReadError(func(err error) {
			require.ErrorIs(t, err, context.DeadlineExceeded)
			atomic.AddInt32(&onReadErrorCalls, 1)
		}))
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onReadErrorCalls) > 0
	}, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestShouldTimeoutWhenPublishingMessagesTakesTooLong(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var onPublishCalls int32 = 0
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(ctx context.Context, _ outbox.Message) {
			require.Equal(t, context.DeadlineExceeded, ctx.Err())
			atomic.AddInt32(&onPublishCalls, 1)
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithPublishTimeout(0), // context should be cancelled
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onPublishCalls) > 0
	}, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestShouldTimeoutWhenDeletingMessagesTakesTooLong(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var onDeleteErrorCalls int32 = 0
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithOnDeleteError(func(_ outbox.Message, err error) {
			require.ErrorIs(t, err, context.DeadlineExceeded)
			atomic.AddInt32(&onDeleteErrorCalls, 1)
		}),
		outbox.WithDeleteTimeout(0), // context should be cancelled
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onDeleteErrorCalls) > 0
	}, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestShouldKeepTryingToPublishMessagesAfterError(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var onPublishCalls int32 = 0
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ outbox.Message) {
			atomic.AddInt32(&onPublishCalls, 1)
		},
		publishErr: errors.New("any error during publish"),
	}, outbox.WithInterval(readerInterval))
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onPublishCalls) > 1
	}, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestStopCancelsInProgressPublishing(t *testing.T) {
	setupTest(t)

	maxMessages := 30
	for range maxMessages {
		writeMessage(t, createMessageFixture())
	}

	var onPublishCalls int32 = 0
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ outbox.Message) {
			atomic.AddInt32(&onPublishCalls, 1)
			time.Sleep(1 * time.Millisecond)
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithMaxMessages(maxMessages),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onPublishCalls) > 0
	}, testTimeout, pollInterval)

	err := r.Stop(context.Background())
	require.NoError(t, err)

	count, err := countMessages(t)
	require.NoError(t, err)
	require.Greater(t, count, 0)
}

func TestMultipleStartCalls(t *testing.T) {
	setupTest(t)

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{})

	r.Start()
	r.Start() // Second call to Start should be a no-op

	err := r.Stop(context.Background())
	require.NoError(t, err)
}

func TestMultipleStopCalls(t *testing.T) {
	setupTest(t)

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{})
	r.Start()

	err := r.Stop(context.Background())
	require.NoError(t, err)

	err = r.Stop(context.Background()) // Second call to Stop should be a no-op
	require.NoError(t, err)
}

func setupTest(t *testing.T) {
	t.Helper()

	err := truncateOutboxTable()
	require.NoError(t, err)
}

func countMessages(t *testing.T) (int, error) {
	t.Helper()

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM Outbox").Scan(&count)
	return count, err
}

func writeMessage(t *testing.T, msg outbox.Message) {
	t.Helper()

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)
	err := w.Write(context.Background(), msg, func(_ context.Context, _ outbox.TxExecFunc) error {
		return nil
	})
	require.NoError(t, err)
}

func writeMessages(t *testing.T, msgs []outbox.Message) {
	t.Helper()

	for _, msg := range msgs {
		writeMessage(t, msg)
	}
}
