package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeTx struct {
	execErr     error
	commitErr   error
	rollbackErr error

	execCalled bool
	committed  bool
	rolledBack bool
}

func (f *fakeTx) ExecContext(ctx context.Context, query string, args ...any) error {
	f.execCalled = true
	return f.execErr
}

func (f *fakeTx) Commit() error {
	f.committed = true
	return f.commitErr
}

func (f *fakeTx) Rollback() error {
	f.rolledBack = true
	return f.rollbackErr
}

type fakeTxProvider struct {
	beginErr error
	tx       *fakeTx
}

func (f *fakeTxProvider) Begin() (Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	return f.tx, nil
}

func TestWriterSucceed(t *testing.T) {
	txProvider := &fakeTxProvider{tx: &fakeTx{}}
	writer := &Writer{txProvider: txProvider}

	err := writer.Write(context.Background(), Message{}, func(ctx context.Context, tx Tx) error {
		return nil
	})

	require.NoError(t, err)

	require.True(t, txProvider.tx.execCalled)
	require.True(t, txProvider.tx.committed)
	require.False(t, txProvider.tx.rolledBack)
}

func TestWriterErrorOnTxBegin(t *testing.T) {
	txProvider := &fakeTxProvider{beginErr: errors.New("failed to begin transaction"), tx: &fakeTx{}}
	writer := &Writer{txProvider: txProvider}

	err := writer.Write(context.Background(), Message{}, func(ctx context.Context, tx Tx) error {
		require.Fail(t, "should not be called")
		return nil
	})

	require.Error(t, err)
	require.Equal(t, txProvider.beginErr, err)

	require.False(t, txProvider.tx.execCalled)
	require.False(t, txProvider.tx.committed)
	require.False(t, txProvider.tx.rolledBack)
}

func TestWriterErrorOnTxCommit(t *testing.T) {
	txProvider := &fakeTxProvider{tx: &fakeTx{commitErr: errors.New("failed to commit transaction")}}
	writer := &Writer{txProvider: txProvider}

	err := writer.Write(context.Background(), Message{}, func(ctx context.Context, tx Tx) error {
		return nil
	})

	require.Error(t, err)
	require.Equal(t, txProvider.tx.commitErr, err)

	require.True(t, txProvider.tx.execCalled)
	require.True(t, txProvider.tx.rolledBack)
}
