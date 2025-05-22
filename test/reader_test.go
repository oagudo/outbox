package test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oagudo/outbox/pkg/outbox"
	"github.com/stretchr/testify/require"
)

func TestReaderSuccessfullyPublishesMessage(t *testing.T) {
	_ = truncateOutboxTable()

	anyMsg := createMessageFixture()

	w := outbox.NewWriter(db)
	err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
		return nil
	})
	require.NoError(t, err)

	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(msg outbox.Message) {
			assertMessageEqual(t, anyMsg, msg)
		},
	}, outbox.WithInterval(10*time.Millisecond))
	r.Start()

	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, anyMsg.ID)
		return !found
	}, 1*time.Second, 50*time.Millisecond)

	r.Stop()
}

func TestReaderPublishesMessagesInOrder(t *testing.T) {
	_ = truncateOutboxTable()

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

	w := outbox.NewWriter(db)
	for _, msg := range msgs {
		err := w.Write(context.Background(), msg, func(_ context.Context, _ outbox.TxExecFunc) error {
			return nil
		})
		require.NoError(t, err)
	}

	var nCalls int32 = 0
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(msg outbox.Message) {
			currentCalls := atomic.LoadInt32(&nCalls)
			require.Equal(t, msg.ID, msgs[currentCalls].ID) // they are published in order
			atomic.AddInt32(&nCalls, 1)
		},
	},
		outbox.WithInterval(10*time.Millisecond),
		outbox.WithMaxMessages(1),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&nCalls) == int32(len(msgs)) //nolint:gosec
	}, 1*time.Second, 50*time.Millisecond)
	r.Stop()
}

func TestReaderOnReadError(t *testing.T) {
	_ = truncateOutboxTable()

	t.Cleanup(func() {
		_, err := db.Exec("ALTER TABLE Outbox_old RENAME TO Outbox")
		require.NoError(t, err)
	})
	_, err := db.Exec("ALTER TABLE Outbox RENAME TO Outbox_old") // force an error on read
	require.NoError(t, err)

	var onReadCallbackCalled atomic.Bool
	r := outbox.NewReader(db, &fakePublisher{}, outbox.WithInterval(10*time.Millisecond),
		outbox.WithOnReadError(func(err error) {
			require.Error(t, err)
			onReadCallbackCalled.Store(true)
		}))
	r.Start()

	require.Eventually(t, onReadCallbackCalled.Load, 1*time.Second, 50*time.Millisecond)
	r.Stop()
}

func TestReaderOnDeleteError(t *testing.T) {
	_ = truncateOutboxTable()

	t.Cleanup(func() {
		_, err := db.Exec("ALTER TABLE Outbox_old RENAME TO Outbox")
		require.NoError(t, err)
	})

	w := outbox.NewWriter(db)
	anyMsg := createMessageFixture()

	err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
		return nil
	})
	require.NoError(t, err)

	var onDeleteCallbackCalled atomic.Bool
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(_ outbox.Message) {
			_, err := db.Exec("ALTER TABLE Outbox RENAME TO Outbox_old") // force an error on delete
			require.NoError(t, err)
		},
	}, outbox.WithInterval(10*time.Millisecond), outbox.WithOnDeleteError(func(_ outbox.Message, err error) {
		require.Error(t, err)
		onDeleteCallbackCalled.Store(true)
	}))
	r.Start()
	require.Eventually(t, onDeleteCallbackCalled.Load, 1*time.Second, 50*time.Millisecond)
	r.Stop()
}
