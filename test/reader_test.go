package test

import (
	"context"
	"testing"
	"time"

	"github.com/oagudo/outbox/pkg/outbox"
	"github.com/stretchr/testify/require"
)

func TestReaderSuccessfullyPublishesMessage(t *testing.T) {

	r := outbox.NewReader(db, &fakePublisher{}, outbox.WithInterval(10*time.Millisecond))
	r.Start()
	w := outbox.NewWriter(db)

	anyMsg := createMessageFixture()

	err := w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.QueryExecutor) error {
		return nil
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, found := readOutboxMessage(t, anyMsg.ID)
		return !found
	}, 1*time.Second, 50*time.Millisecond)
	r.Stop()
}

func TestReaderPublishesMessagesInOrder(t *testing.T) {
	firstMsg := createMessageFixture()
	secondMsg := createMessageFixture()
	thirdMsg := createMessageFixture()

	msgs := []outbox.Message{
		firstMsg,
		secondMsg,
		thirdMsg,
	}

	w := outbox.NewWriter(db)
	for _, msg := range msgs {
		err := w.Write(context.Background(), msg, func(_ context.Context, _ outbox.QueryExecutor) error {
			return nil
		})
		require.NoError(t, err)
	}

	nCalls := 0
	r := outbox.NewReader(db, &fakePublisher{
		onPublish: func(msg outbox.Message) {
			require.Equal(t, msg.ID, msgs[nCalls].ID) // they are published in order
			nCalls++
		},
	},
		outbox.WithInterval(10*time.Millisecond),
		outbox.WithMaxMessages(1),
	)
	r.Start()

	require.Eventually(t, func() bool {
		return nCalls == len(msgs)
	}, 1*time.Second, 50*time.Millisecond)
	r.Stop()
}
