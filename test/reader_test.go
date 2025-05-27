package test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/oagudo/outbox/pkg/outbox"
	"github.com/stretchr/testify/assert"
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
		onPublish: func(_ context.Context, msg outbox.Message) error {
			assertMessageEqual(t, anyMsg, msg)
			return nil
		},
	}, outbox.WithInterval(readerInterval))
	r.Start()

	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, anyMsg.ID)
		return !found
	}, testTimeout, pollInterval)

	require.NoError(t, r.Stop(context.Background()))
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
		onPublish: func(_ context.Context, msg outbox.Message) error {
			currentCalls := atomic.AddInt32(&onPublishCalls, 1)
			require.Equal(t, msg.ID, msgs[currentCalls-1].ID) // they are published in order
			return nil
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithMaxMessages(1),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onPublishCalls) == int32(len(msgs)) //nolint:gosec
	}, testTimeout, pollInterval)

	require.NoError(t, r.Stop(context.Background()))
}

func TestStopTimesOutIfReaderIsGracefullyStopped(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var wg sync.WaitGroup
	wg.Add(1)
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ outbox.Message) error {
			wg.Done() // trigger for stop
			time.Sleep(100 * time.Millisecond)
			return nil
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

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithReadTimeout(0), // context should be cancelled
	)
	r.Start()

	waitForReaderError(t, r, outbox.OpRead, context.DeadlineExceeded, nil)

	require.NoError(t, r.Stop(context.Background()))
}

func TestShouldTimeoutWhenPublishingMessagesTakesTooLong(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithPublishTimeout(0), // context should be cancelled
	)

	r.Start()

	waitForReaderError(t, r, outbox.OpPublish, context.DeadlineExceeded, &anyMsg.ID)

	require.NoError(t, r.Stop(context.Background()))
}

func TestShouldTimeoutWhenDeletingMessagesTakesTooLong(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithDeleteTimeout(0), // context should be cancelled
	)
	r.Start()

	waitForReaderError(t, r, outbox.OpDelete, context.DeadlineExceeded, &anyMsg.ID)

	require.NoError(t, r.Stop(context.Background()))
}

func TestShouldKeepTryingToPublishMessagesAfterError(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	publishErr := errors.New("any error during publish")

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ outbox.Message) error {
			return publishErr
		},
	}, outbox.WithInterval(readerInterval))
	r.Start()

	waitForReaderError(t, r, outbox.OpPublish, publishErr, &anyMsg.ID)
	waitForReaderError(t, r, outbox.OpPublish, publishErr, &anyMsg.ID)
	waitForReaderError(t, r, outbox.OpPublish, publishErr, &anyMsg.ID)

	require.NoError(t, r.Stop(context.Background()))
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
		onPublish: func(_ context.Context, _ outbox.Message) error {
			atomic.AddInt32(&onPublishCalls, 1)
			time.Sleep(1 * time.Millisecond)
			return nil
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithMaxMessages(maxMessages),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onPublishCalls) > 0
	}, testTimeout, pollInterval)

	require.NoError(t, r.Stop(context.Background()))

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

	require.NoError(t, r.Stop(context.Background()))
}

func TestMultipleStopCalls(t *testing.T) {
	setupTest(t)

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{})
	r.Start()

	require.NoError(t, r.Stop(context.Background()))
	require.NoError(t, r.Stop(context.Background())) // Second call to Stop should be a no-op
}

func TestReaderDiscardsErrorsIfBufferIsFull(t *testing.T) {
	setupTest(t)

	writeMessage(t, createMessageFixture())

	firstErr := errors.New("first error during publish")
	secondErr := errors.New("second error during publish")
	subsequentErr := errors.New("subsequent error during publish")

	wg := sync.WaitGroup{}
	wg.Add(1)
	onPublishCalls := 0
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ outbox.Message) error {
			onPublishCalls++
			if onPublishCalls == 1 {
				return firstErr
			}
			if onPublishCalls == 2 {
				return secondErr
			}
			if onPublishCalls == 3 {
				wg.Done()
			}
			return subsequentErr
		},
	},
		outbox.WithErrorChannelSize(1),
		outbox.WithInterval(readerInterval),
	)
	r.Start()

	wg.Wait()

	waitForReaderError(t, r, outbox.OpPublish, firstErr, nil)
	waitForReaderError(t, r, outbox.OpPublish, subsequentErr, nil) // second error is be discarded

	require.NoError(t, r.Stop(context.Background()))
}

func setupTest(t *testing.T) {
	t.Helper()

	require.NoError(t, truncateOutboxTable())
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

func waitForReaderError(t *testing.T, r *outbox.Reader,
	expectedOp outbox.OpKind, expectedErr error, expectedMsgID *uuid.UUID,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case err, ok := <-r.Errors():
			if !ok { // channel closed by Reader
				return false
			}
			if err.Op != expectedOp {
				return false
			}

			assert.ErrorIs(t, err.Err, expectedErr,
				"expected error to match expected type")

			if expectedMsgID != nil {
				assert.Equal(t, *expectedMsgID, err.Msg.ID,
					"expected error message ID to match")
			}

			return true
		default:
			return false
		}
	}, testTimeout, pollInterval)
}
