package outbox

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type fakeDB struct {
	beginTxErr error
	tx         *fakeTx
}

func (f *fakeDB) BeginTx(_ context.Context, _ *sql.TxOptions) (Tx, error) {
	if f.beginTxErr != nil {
		return nil, f.beginTxErr
	}
	return f.tx, nil
}

func (f *fakeDB) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (f *fakeDB) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

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

func (f *fakeTx) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (f *fakeTx) Commit() error {
	f.committed = true
	return f.commitErr
}

func (f *fakeTx) Rollback() error {
	f.rolledBack = true
	return f.rollbackErr
}

func TestWriterSucceed(t *testing.T) {
	tx := &fakeTx{}
	db := &fakeDB{tx: tx}
	writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

	var callbackCalled bool
	err := writer.Write(context.Background(), &Message{}, func(_ context.Context, _ TxQueryer) error {
		callbackCalled = true
		return nil
	})

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	if !tx.execCalled {
		t.Fatal("expected tx.ExecContext to be called")
	}
	if tx.rolledBack {
		t.Fatal("expected tx not to be rolled back")
	}
	if !tx.committed {
		t.Fatal("expected tx to be committed")
	}
}

func TestWriterErrorOnTxBegin(t *testing.T) {
	tx := &fakeTx{}
	db := &fakeDB{beginTxErr: errors.New("failed to begin transaction"), tx: tx}
	writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

	err := writer.Write(context.Background(), &Message{}, func(_ context.Context, _ TxQueryer) error {
		t.Fatal("should not be called")
		return nil
	})

	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, db.beginTxErr) {
		t.Fatalf("expected error to be %v, got: %v", db.beginTxErr, err)
	}

	if tx.execCalled {
		t.Fatal("expected tx.ExecContext not to be called")
	}
	if tx.committed {
		t.Fatal("expected tx not to be committed")
	}
	if tx.rolledBack {
		t.Fatal("expected tx not to be rolled back")
	}
}

func TestWriterErrorOnTxCommit(t *testing.T) {
	tx := &fakeTx{commitErr: errors.New("failed to commit transaction")}
	db := &fakeDB{tx: tx}
	writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

	var callbackCalled bool
	err := writer.Write(context.Background(), &Message{}, func(_ context.Context, _ TxQueryer) error {
		callbackCalled = true
		return nil
	})

	if !errors.Is(err, tx.commitErr) {
		t.Fatalf("expected error to be %v, got: %v", tx.commitErr, err)
	}

	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	if !tx.execCalled {
		t.Fatal("expected tx.ExecContext to be called")
	}
	if !tx.rolledBack {
		t.Fatal("expected tx to be rolled back")
	}
}
