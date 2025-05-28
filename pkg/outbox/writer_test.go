package outbox

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	coreSql "github.com/oagudo/outbox/internal/sql"
)

type fakeTx struct {
	execErr     error
	commitErr   error
	rollbackErr error

	execCalled bool
	committed  bool
	rolledBack bool
}

func (f *fakeTx) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	f.execCalled = true
	return nil, f.execErr
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

func (f *fakeTxProvider) BeginTx() (coreSql.Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	return f.tx, nil
}

func (f *fakeTxProvider) ExecContext(_ context.Context, _ string, _ ...any) error {
	return nil
}

func TestWriterSucceed(t *testing.T) {
	txProvider := &fakeTxProvider{tx: &fakeTx{}}
	writer := &Writer{sqlExecutor: txProvider, dbCtx: &DBContext{dialect: SQLDialectPostgres}}

	var callbackCalled bool
	err := writer.Write(context.Background(), Message{}, func(_ context.Context, _ ExecInTxFunc) error {
		callbackCalled = true
		return nil
	})

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	if !txProvider.tx.execCalled {
		t.Fatal("expected tx.ExecContext to be called")
	}
	if txProvider.tx.rolledBack {
		t.Fatal("expected tx not to be rolled back")
	}
	if !txProvider.tx.committed {
		t.Fatal("expected tx to be committed")
	}
}

func TestWriterErrorOnTxBegin(t *testing.T) {
	txProvider := &fakeTxProvider{beginErr: errors.New("failed to begin transaction"), tx: &fakeTx{}}
	writer := &Writer{sqlExecutor: txProvider, dbCtx: &DBContext{dialect: SQLDialectPostgres}}

	err := writer.Write(context.Background(), Message{}, func(_ context.Context, _ ExecInTxFunc) error {
		t.Fatal("should not be called")
		return nil
	})

	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, txProvider.beginErr) {
		t.Fatalf("expected error to be %v, got: %v", txProvider.beginErr, err)
	}

	if txProvider.tx.execCalled {
		t.Fatal("expected tx.ExecContext not to be called")
	}
	if txProvider.tx.committed {
		t.Fatal("expected tx not to be committed")
	}
	if txProvider.tx.rolledBack {
		t.Fatal("expected tx not to be rolled back")
	}
}

func TestWriterErrorOnTxCommit(t *testing.T) {
	txProvider := &fakeTxProvider{tx: &fakeTx{commitErr: errors.New("failed to commit transaction")}}
	writer := &Writer{sqlExecutor: txProvider, dbCtx: &DBContext{dialect: SQLDialectPostgres}}

	var callbackCalled bool
	err := writer.Write(context.Background(), Message{}, func(_ context.Context, _ ExecInTxFunc) error {
		callbackCalled = true
		return nil
	})

	if !errors.Is(err, txProvider.tx.commitErr) {
		t.Fatalf("expected error to be %v, got: %v", txProvider.tx.commitErr, err)
	}

	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	if !txProvider.tx.execCalled {
		t.Fatal("expected tx.ExecContext to be called")
	}
	if !txProvider.tx.rolledBack {
		t.Fatal("expected tx to be rolled back")
	}
}
