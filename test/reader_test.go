package test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oagudo/outbox/pkg/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	readerInterval = 10 * time.Millisecond
	testTimeout    = 2 * time.Second
	pollInterval   = 10 * time.Millisecond
)

func TestReaderSuccessfullyPublishesMessage(t *testing.T) {
	dbCtx := setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg *outbox.Message) error {
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
	dbCtx := setupTest(t)

	firstMsg := createMessageFixture()
	secondMsg := createMessageFixture(outbox.WithCreatedAt(firstMsg.CreatedAt.Add(1 * time.Second)))
	thirdMsg := createMessageFixture(outbox.WithCreatedAt(firstMsg.CreatedAt.Add(2 * time.Second)))

	msgs := []*outbox.Message{
		firstMsg,
		secondMsg,
		thirdMsg,
	}

	writeMessages(t, msgs)

	var onPublishCalls int32 = 0
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg *outbox.Message) error {
			currentCalls := atomic.LoadInt32(&onPublishCalls)
			require.Equal(t, msg.ID, msgs[currentCalls].ID) // they are published in order
			atomic.AddInt32(&onPublishCalls, 1)
			return nil
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithReadBatchSize(1),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return int(atomic.LoadInt32(&onPublishCalls)) == len(msgs)
	}, testTimeout, pollInterval)

	require.NoError(t, r.Stop(context.Background()))
}

func TestReaderRetriesFailedPublishAndRetainsMessage(t *testing.T) {
	dbCtx := setupTest(t)

	firstPublishedMsg := createMessageFixture()

	failingMsg := createMessageFixture()
	failingMsg.CreatedAt = firstPublishedMsg.CreatedAt.Add(1 * time.Second)

	secondPublishedMsg := createMessageFixture()
	secondPublishedMsg.CreatedAt = firstPublishedMsg.CreatedAt.Add(2 * time.Second)

	writeMessages(t, []*outbox.Message{firstPublishedMsg, failingMsg, secondPublishedMsg})

	publishErr := errors.New("any error during publish")

	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg *outbox.Message) error {
			if msg.ID == failingMsg.ID {
				return publishErr
			}
			return nil
		},
	}, outbox.WithInterval(readerInterval),
		outbox.WithDelay(0))
	r.Start()

	waitForReaderError(t, r, outbox.OpPublish, publishErr)
	waitForReaderError(t, r, outbox.OpPublish, publishErr)
	waitForReaderError(t, r, outbox.OpPublish, publishErr)

	require.Equal(t, 1, countMessages(t))

	msg, found := readOutboxMessage(t, failingMsg.ID)
	require.True(t, found)
	assertMessageEqual(t, failingMsg, msg)

	require.NoError(t, r.Stop(context.Background()))
}

func TestStopTimesOutIfReaderIsGracefullyStopped(t *testing.T) {
	dbCtx := setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var wg sync.WaitGroup
	wg.Add(1)
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			wg.Done() // trigger for stop
			time.Sleep(readerInterval * 2)
			return nil
		},
	}, outbox.WithInterval(readerInterval))
	r.Start()

	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), readerInterval)
	defer cancel()

	err := r.Stop(ctx)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestShouldTimeoutWhenReadingMessagesTakesTooLong(t *testing.T) {
	dbCtx := setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithReadTimeout(0), // context should be cancelled
	)
	r.Start()

	waitForReaderError(t, r, outbox.OpRead, context.DeadlineExceeded)

	require.NoError(t, r.Stop(context.Background()))
}

func TestShouldTimeoutWhenPublishingMessagesTakesTooLong(t *testing.T) {
	dbCtx := setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithPublishTimeout(0), // context should be cancelled
	)

	r.Start()

	waitForReaderError(t, r, outbox.OpPublish, context.DeadlineExceeded)

	require.NoError(t, r.Stop(context.Background()))
}

func TestShouldTimeoutWhenDeletingMessagesAtTheEndOfBatchTakesTooLong(t *testing.T) {
	dbCtx := setupTest(t)

	writeMessage(t, createMessageFixture())

	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithDeleteTimeout(0), // context should be cancelled
	)
	r.Start()

	waitForReaderError(t, r, outbox.OpDelete, context.DeadlineExceeded)

	require.NoError(t, r.Stop(context.Background()))
}

func TestShouldTimeoutWhenDeletingMessagesDuringBatchIterationTakesTooLong(t *testing.T) {
	dbCtx := setupTest(t)

	writeMessages(t, []*outbox.Message{createMessageFixture(), createMessageFixture()})

	r := outbox.NewReader(dbCtx, &fakePublisher{},
		outbox.WithInterval(readerInterval),
		outbox.WithDeleteTimeout(0), // context should be cancelled
		outbox.WithDeleteBatchSize(1),
	)
	r.Start()

	waitForReaderError(t, r, outbox.OpDelete, context.DeadlineExceeded)

	require.NoError(t, r.Stop(context.Background()))
}

func TestShouldTimeoutWhenUpdatingMessagesTakesTooLong(t *testing.T) {
	dbCtx := setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			return errors.New("any error during publish")
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithUpdateTimeout(0), // context should be cancelled
	)
	r.Start()

	waitForReaderError(t, r, outbox.OpUpdate, context.DeadlineExceeded)

	require.NoError(t, r.Stop(context.Background()))
}

func TestStopCancelsInProgressPublishing(t *testing.T) {
	dbCtx := setupTest(t)

	maxMessages := 30
	for range maxMessages {
		writeMessage(t, createMessageFixture())
	}

	var onPublishCalls int32 = 0
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			atomic.AddInt32(&onPublishCalls, 1)
			time.Sleep(1 * time.Millisecond)
			return nil
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithReadBatchSize(maxMessages),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&onPublishCalls) > 0
	}, testTimeout, pollInterval)

	require.NoError(t, r.Stop(context.Background()))

	require.Greater(t, countMessages(t), 0)
}

func TestMultipleStartCalls(t *testing.T) {
	dbCtx := setupTest(t)

	r := outbox.NewReader(dbCtx, &fakePublisher{})

	r.Start()
	r.Start() // Second call to Start should be a no-op

	require.NoError(t, r.Stop(context.Background()))
}

func TestMultipleStopCalls(t *testing.T) {
	dbCtx := setupTest(t)

	r := outbox.NewReader(dbCtx, &fakePublisher{})
	r.Start()

	require.NoError(t, r.Stop(context.Background()))
	require.NoError(t, r.Stop(context.Background())) // Second call to Stop should be a no-op
}

func TestReaderDiscardsErrorsIfBufferIsFull(t *testing.T) {
	dbCtx := setupTest(t)

	writeMessage(t, createMessageFixture())

	firstErr := errors.New("first error during publish")
	secondErr := errors.New("second error during publish")
	subsequentErr := errors.New("subsequent error during publish")

	wg := sync.WaitGroup{}
	wg.Add(1)
	onPublishCalls := 0
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
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
		outbox.WithDelay(0),
	)
	r.Start()

	wg.Wait()

	waitForReaderError(t, r, outbox.OpPublish, firstErr)
	waitForReaderError(t, r, outbox.OpPublish, subsequentErr) // second error is be discarded

	require.NoError(t, r.Stop(context.Background()))
}

func TestReaderDropsDiscardedMessagesWhenChannelIsFull(t *testing.T) {
	dbCtx := setupTest(t)

	firstMessageDiscarded := createMessageFixture()
	secondMessageDiscarded := createMessageFixture(outbox.WithCreatedAt(firstMessageDiscarded.CreatedAt.Add(1 * time.Second)))

	writeMessages(t, []*outbox.Message{firstMessageDiscarded, secondMessageDiscarded})

	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			return errors.New("any error during publish")
		},
	},
		outbox.WithDiscardedMessagesChannelSize(1),
		outbox.WithMaxAttempts(1),
		outbox.WithInterval(readerInterval),
		outbox.WithDelay(0),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return countMessages(t) == 0
	}, testTimeout, pollInterval)

	waitForReaderDiscardedMessage(t, r, firstMessageDiscarded)

	thirdMessageDiscarded := createMessageFixture()
	writeMessage(t, thirdMessageDiscarded)

	waitForReaderDiscardedMessage(t, r, thirdMessageDiscarded)

	require.NoError(t, r.Stop(context.Background()))
}

func TestReaderDeletesMessagesInBatches(t *testing.T) {
	dbCtx := setupTest(t)

	deleteBatchSize := 10
	for range deleteBatchSize {
		writeMessage(t, createMessageFixture())
	}

	done := make(chan struct{})
	var onPublishCalls int32 = 0
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			numberOfCalls := atomic.AddInt32(&onPublishCalls, 1)
			if int(numberOfCalls) == deleteBatchSize-1 {
				// wait before the batch size is reached
				<-done
			}
			return nil
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithDeleteBatchSize(deleteBatchSize),
		outbox.WithReadBatchSize(deleteBatchSize*2),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return countMessages(t) == deleteBatchSize
	}, testTimeout, pollInterval)

	close(done)

	require.Eventually(t, func() bool {
		return countMessages(t) == 0
	}, testTimeout, pollInterval)

	require.NoError(t, r.Stop(context.Background()))
}

func TestReaderDeletesAllPublishedMessagesAfterIterationEvenIfBatchSizeIsNotReached(t *testing.T) {
	dbCtx := setupTest(t)

	deleteBatchSize := 10
	halfBatchSize := deleteBatchSize / 2
	for range halfBatchSize {
		writeMessage(t, createMessageFixture())
	}

	var onPublishCalls int32 = 0
	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			atomic.AddInt32(&onPublishCalls, 1)
			return nil
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithDeleteBatchSize(deleteBatchSize),
		outbox.WithReadBatchSize(deleteBatchSize*2),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return countMessages(t) == 0
	}, testTimeout, pollInterval)

	require.Equal(t, halfBatchSize, int(atomic.LoadInt32(&onPublishCalls)))

	require.NoError(t, r.Stop(context.Background()))
}

func TestReaderDiscardsMessageAfterMaxAttempts(t *testing.T) {
	dbCtx := setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var numberOfPublishAttempts int32 = 0
	reader := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg *outbox.Message) error {
			assert.Equal(t, anyMsg.ID, msg.ID)
			atomic.AddInt32(&numberOfPublishAttempts, 1)
			return errors.New("any error during publish")
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithReadBatchSize(1),
		outbox.WithMaxAttempts(3),
		outbox.WithDelay(0),
	)
	reader.Start()

	waitForReaderDiscardedMessage(t, reader, anyMsg)

	require.Equal(t, 3, int(atomic.LoadInt32(&numberOfPublishAttempts)))

	require.Eventually(t, func() bool {
		return countMessages(t) == 0
	}, testTimeout, pollInterval)

	require.NoError(t, reader.Stop(context.Background()))
}

func TestReaderDiscardsMessageAfterOptimisticPublishFailure(t *testing.T) {
	dbCtx := setupTest(t)

	anyMsg := createMessageFixture()
	writer := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(&fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			return errors.New("any error during optimistic publish")
		},
	}))
	err := writer.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.ExecInTxFunc) error {
		return nil
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		savedMessage, found := readOutboxMessage(t, anyMsg.ID)
		return found && savedMessage.TimesAttempted == 1
	}, testTimeout, pollInterval)

	reader := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, _ *outbox.Message) error {
			require.Fail(t, "should not be called")
			return nil
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithReadBatchSize(1),
		outbox.WithMaxAttempts(1),
		outbox.WithDelay(0),
	)
	reader.Start()

	waitForReaderDiscardedMessage(t, reader, anyMsg)

	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, anyMsg.ID)
		return !found
	}, testTimeout, pollInterval)

	require.NoError(t, reader.Stop(context.Background()))
}

func TestReaderRetriesWithFixedDelay(t *testing.T) {
	dbCtx := setupTest(t)

	anyDelay := 5 * time.Millisecond

	failingMsg := createMessageFixture()
	writeMessage(t, failingMsg)

	publishErr := errors.New("any error during publish")
	var scheduledTimes []time.Time

	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg *outbox.Message) error {
			scheduledTimes = append(scheduledTimes, msg.ScheduledAt)
			return publishErr
		},
	}, outbox.WithInterval(readerInterval),
		outbox.WithDelay(anyDelay),
		outbox.WithDelayStrategy(outbox.DelayStrategyFixed),
		outbox.WithMaxAttempts(4),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return countMessages(t) == 0
	}, testTimeout, pollInterval)

	require.Equal(t, 4, len(scheduledTimes))

	require.True(t, failingMsg.CreatedAt.Equal(scheduledTimes[0]))
	require.Equal(t, scheduledTimes[1].Sub(scheduledTimes[0]), anyDelay)
	require.Equal(t, scheduledTimes[2].Sub(scheduledTimes[1]), anyDelay)
	require.Equal(t, scheduledTimes[3].Sub(scheduledTimes[2]), anyDelay)

	require.NoError(t, r.Stop(context.Background()))
}

func TestReaderRetriesWithExponentialDelay(t *testing.T) {
	dbCtx := setupTest(t)

	anyDelay := 10 * time.Millisecond

	failingMsg := createMessageFixture()
	writeMessage(t, failingMsg)

	publishErr := errors.New("any error during publish")
	var scheduledTimes []time.Time

	r := outbox.NewReader(dbCtx, &fakePublisher{
		onPublish: func(_ context.Context, msg *outbox.Message) error {
			scheduledTimes = append(scheduledTimes, msg.ScheduledAt)
			return publishErr
		},
	}, outbox.WithInterval(readerInterval),
		outbox.WithDelayStrategy(outbox.DelayStrategyExponential),
		outbox.WithDelay(anyDelay),
		outbox.WithMaxDelay(anyDelay*4),
		outbox.WithMaxAttempts(5),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return countMessages(t) == 0
	}, testTimeout, pollInterval)

	require.Equal(t, 5, len(scheduledTimes))

	require.True(t, failingMsg.CreatedAt.Equal(scheduledTimes[0]))
	require.Equal(t, scheduledTimes[1].Sub(scheduledTimes[0]), anyDelay)
	require.Equal(t, scheduledTimes[2].Sub(scheduledTimes[1]), anyDelay*2)
	require.Equal(t, scheduledTimes[3].Sub(scheduledTimes[2]), anyDelay*4)
	require.Equal(t, scheduledTimes[4].Sub(scheduledTimes[3]), anyDelay*4) // max delay is reached

	require.NoError(t, r.Stop(context.Background()))
}

func TestReaderDoesNotPickMessagesFromScheduledInTheFuture(t *testing.T) {
	dbCtx := setupTest(t)

	futureMsg := createMessageFixture(outbox.WithScheduledAt(time.Now().UTC().Add(1 * time.Hour)))

	writeMessages(t, []*outbox.Message{
		createMessageFixture(),
		createMessageFixture(),
		createMessageFixture(),
		createMessageFixture(),
		futureMsg,
	})

	r := outbox.NewReader(dbCtx, &fakePublisher{}, outbox.WithInterval(readerInterval))
	r.Start()

	require.Eventually(t, func() bool {
		return countMessages(t) == 1
	}, testTimeout, pollInterval)

	savedMsg, found := readOutboxMessage(t, futureMsg.ID)
	require.True(t, found)
	assertMessageEqual(t, futureMsg, savedMsg)

	require.NoError(t, r.Stop(context.Background()))
}

func setupTest(t *testing.T) *outbox.DBContext {
	t.Helper()

	require.NoError(t, truncateOutboxTable())

	return outbox.NewDBContext(db, outbox.SQLDialectPostgres)
}

func countMessages(t *testing.T) int {
	t.Helper()

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM Outbox").Scan(&count)
	require.NoError(t, err)
	return count
}

func writeMessage(t *testing.T, msg *outbox.Message) {
	t.Helper()

	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	w := outbox.NewWriter(dbCtx)
	err := w.Write(context.Background(), msg, func(_ context.Context, _ outbox.ExecInTxFunc) error {
		return nil
	})
	require.NoError(t, err)
}

func writeMessages(t *testing.T, msgs []*outbox.Message) {
	t.Helper()

	for _, msg := range msgs {
		writeMessage(t, msg)
	}
}

func waitForReaderError(t *testing.T, r *outbox.Reader, expectedOp outbox.OpKind, expectedErr error) {
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

			return true
		default:
			return false
		}
	}, testTimeout, pollInterval)
}

func waitForReaderDiscardedMessage(t *testing.T, r *outbox.Reader, expectedMsg *outbox.Message) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case msg, ok := <-r.DiscardedMessages():
			if !ok { // channel closed by Reader
				return false
			}
			assertMessageEqual(t, expectedMsg, &msg)
			return true
		default:
			return false
		}
	}, testTimeout, pollInterval)
}
