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

	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(msg outbox.Message) {
			assertMessageEqual(t, anyMsg, msg)
		},
	}, outbox.WithInterval(readerInterval))
	r.Start()

	waitForMessageDeletion(t, anyMsg.ID)

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
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(msg outbox.Message) {
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
	r := outbox.NewReader(db, &fakePublisher{}, outbox.WithInterval(readerInterval),
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
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(_ outbox.Message) {
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
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(_ outbox.Message) {
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
	require.Equal(t, err, context.DeadlineExceeded)
}

func TestShouldKeepTryingToPublishMessagesAfterError(t *testing.T) {
	setupTest(t)

	anyMsg := createMessageFixture()
	writeMessage(t, anyMsg)

	var onPublishCalls int32 = 0
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(_ outbox.Message) {
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

	maxMessages := 100
	for range maxMessages {
		writeMessage(t, createMessageFixture())
	}

	var wg sync.WaitGroup
	wg.Add(1)
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(_ outbox.Message) {
			wg.Done() // trigger for stop
			time.Sleep(1 * time.Millisecond)
		},
	},
		outbox.WithInterval(readerInterval),
		outbox.WithMaxMessages(maxMessages),
	)
	r.Start()

	wg.Wait()

	err := r.Stop(context.Background())
	require.NoError(t, err)

	count, err := countMessages(t)
	require.NoError(t, err)
	require.Greater(t, count, 0)
}

func TestStartAndStopCalledMultipleTimes(t *testing.T) {
	setupTest(t)

	r := outbox.NewReader(db, &fakePublisher{})

	r.Start()
	r.Start() // should not panic

	err := r.Stop(context.Background())
	require.NoError(t, err)

	err = r.Stop(context.Background())
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

func waitForMessageDeletion(t *testing.T, msgID uuid.UUID) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, msgID)
		return !found
	}, testTimeout, pollInterval, "Message should be deleted from outbox")
}

func writeMessage(t *testing.T, msg outbox.Message) {
	t.Helper()

	w := outbox.NewWriter(db)
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
