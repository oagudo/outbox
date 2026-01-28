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

	execCallCount int
	committed     bool
	rolledBack    bool
}

func (f *fakeTx) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	f.execCallCount++
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

func TestWrite(t *testing.T) {
	t.Run("commits transaction with message", func(t *testing.T) {
		tx := &fakeTx{}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		var callbackCalled bool
		err := writer.Write(context.Background(), func(ctx context.Context, _ TxQueryer, msgWriter MessageWriter) error {
			callbackCalled = true
			return msgWriter.Store(ctx, &Message{})
		})

		assertNoError(t, err)
		if !callbackCalled {
			t.Fatal("expected callback to be called")
		}
		assertExecCallCount(t, tx, 1)
		assertNotRolledBack(t, tx)
		assertCommitted(t, tx)
	})

	t.Run("commits transaction with multiple messages", func(t *testing.T) {
		tx := &fakeTx{}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.Write(context.Background(), func(ctx context.Context, _ TxQueryer, msgWriter MessageWriter) error {
			if err := msgWriter.Store(ctx, &Message{}); err != nil {
				return err
			}
			return msgWriter.Store(ctx, &Message{})
		})

		assertNoError(t, err)
		assertExecCallCount(t, tx, 2)
		assertNotRolledBack(t, tx)
		assertCommitted(t, tx)
	})

	t.Run("commits transaction with zero messages", func(t *testing.T) {
		tx := &fakeTx{}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.Write(context.Background(), func(_ context.Context, _ TxQueryer, _ MessageWriter) error {
			return nil
		})

		assertNoError(t, err)
		assertExecCallCount(t, tx, 0)
		assertNotRolledBack(t, tx)
		assertCommitted(t, tx)
	})

	t.Run("rolls back on callback error", func(t *testing.T) {
		tx := &fakeTx{}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		callbackErr := errors.New("callback error")
		err := writer.Write(context.Background(), func(_ context.Context, _ TxQueryer, _ MessageWriter) error {
			return callbackErr
		})

		assertErrorIs(t, err, callbackErr)
		assertNotCommitted(t, tx)
		assertRolledBack(t, tx)
	})

	t.Run("rolls back on commit error", func(t *testing.T) {
		tx := &fakeTx{commitErr: errors.New("commit error")}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.Write(context.Background(), func(ctx context.Context, _ TxQueryer, msgWriter MessageWriter) error {
			return msgWriter.Store(ctx, &Message{})
		})

		assertErrorIs(t, err, tx.commitErr)
		assertRolledBack(t, tx)
	})

	t.Run("rolls back when message store fails", func(t *testing.T) {
		tx := &fakeTx{execErr: errors.New("insert error")}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.Write(context.Background(), func(ctx context.Context, _ TxQueryer, msgWriter MessageWriter) error {
			return msgWriter.Store(ctx, &Message{})
		})

		assertErrorIs(t, err, tx.execErr)
		assertNotCommitted(t, tx)
		assertRolledBack(t, tx)
	})

	t.Run("returns error on tx begin failure", func(t *testing.T) {
		beginErr := errors.New("begin error")
		db := &fakeDB{beginTxErr: beginErr}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.Write(context.Background(), func(_ context.Context, _ TxQueryer, _ MessageWriter) error {
			t.Fatal("callback should not be called")
			return nil
		})

		assertErrorIs(t, err, beginErr)
	})
}

func TestWriteOne(t *testing.T) {
	t.Run("commits transaction with message", func(t *testing.T) {
		tx := &fakeTx{}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		var callbackCalled bool
		err := writer.WriteOne(context.Background(), &Message{}, func(_ context.Context, _ TxQueryer) error {
			callbackCalled = true
			return nil
		})

		assertNoError(t, err)
		if !callbackCalled {
			t.Fatal("expected callback to be called")
		}
		assertExecCallCount(t, tx, 1)
		assertNotRolledBack(t, tx)
		assertCommitted(t, tx)
	})

	t.Run("rolls back on callback error", func(t *testing.T) {
		tx := &fakeTx{}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		callbackErr := errors.New("callback error")
		err := writer.WriteOne(context.Background(), &Message{}, func(_ context.Context, _ TxQueryer) error {
			return callbackErr
		})

		assertErrorIs(t, err, callbackErr)
		assertNotCommitted(t, tx)
		assertRolledBack(t, tx)
	})

	t.Run("rolls back on commit error", func(t *testing.T) {
		tx := &fakeTx{commitErr: errors.New("commit error")}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		var callbackCalled bool
		err := writer.WriteOne(context.Background(), &Message{}, func(_ context.Context, _ TxQueryer) error {
			callbackCalled = true
			return nil
		})

		assertErrorIs(t, err, tx.commitErr)
		if !callbackCalled {
			t.Fatal("expected callback to be called")
		}
		assertExecCallCount(t, tx, 1)
		assertRolledBack(t, tx)
	})

	t.Run("returns error on tx begin failure", func(t *testing.T) {
		tx := &fakeTx{}
		beginErr := errors.New("begin error")
		db := &fakeDB{beginTxErr: beginErr, tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.WriteOne(context.Background(), &Message{}, func(_ context.Context, _ TxQueryer) error {
			t.Fatal("callback should not be called")
			return nil
		})

		assertErrorIs(t, err, beginErr)
	})
}

func TestUnmanagedWriter_Store(t *testing.T) {
	t.Run("stores message without commit or rollback", func(t *testing.T) {
		tx := &fakeTx{}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.Unmanaged().Store(context.Background(), tx, &Message{})

		assertNoError(t, err)
		assertExecCallCount(t, tx, 1)
		assertNotCommitted(t, tx)
		assertNotRolledBack(t, tx)
	})

	t.Run("returns error on exec failure", func(t *testing.T) {
		tx := &fakeTx{execErr: errors.New("insert error")}
		db := &fakeDB{tx: tx}
		writer := NewWriter(NewDBContextWithDB(db, SQLDialectPostgres))

		err := writer.Unmanaged().Store(context.Background(), tx, &Message{})

		assertErrorIs(t, err, tx.execErr)
		assertNotCommitted(t, tx)
		assertNotRolledBack(t, tx)
	})
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func assertErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("expected error to be %v, got: %v", target, err)
	}
}

func assertCommitted(t *testing.T, tx *fakeTx) {
	t.Helper()
	if !tx.committed {
		t.Fatal("expected tx to be committed")
	}
}

func assertNotCommitted(t *testing.T, tx *fakeTx) {
	t.Helper()
	if tx.committed {
		t.Fatal("expected tx not to be committed")
	}
}

func assertRolledBack(t *testing.T, tx *fakeTx) {
	t.Helper()
	if !tx.rolledBack {
		t.Fatal("expected tx to be rolled back")
	}
}

func assertNotRolledBack(t *testing.T, tx *fakeTx) {
	t.Helper()
	if tx.rolledBack {
		t.Fatal("expected tx not to be rolled back")
	}
}

func assertExecCallCount(t *testing.T, tx *fakeTx, expected int) {
	t.Helper()
	if tx.execCallCount != expected {
		t.Fatalf("expected tx.ExecContext to be called %d time(s), got: %d", expected, tx.execCallCount)
	}
}
