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
