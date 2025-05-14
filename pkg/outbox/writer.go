package outbox

import (
	"context"
	"database/sql"
	"fmt"

	coreSql "github.com/oagudo/outbox/internal/sql"
)

// Writer handles storing messages in the outbox table as part of user-defined queries within a database transaction.
// It optionally supports optimistic publishing, which attempts to publish messages
// immediately after transaction commit.
type Writer struct {
	sqlExecutor  coreSql.Executor
	msgPublisher MessagePublisher
}

// TxExecFunc is a function that executes a SQL query within a transaction.
type TxExecFunc func(ctx context.Context, query string, args ...any) error

// WriterTxFunc is a function that executes user-defined queries within the transaction that
// stores a message in the outbox.
type WriterTxFunc func(ctx context.Context, txExecFunc TxExecFunc) error

// WriterOption is a function that configures a Writer instance.
type WriterOption func(*Writer)

// WithOptimisticPublisher configures the Writer to attempt immediate publishing
// of messages after the transaction is committed.
// This can improve performance by reducing the delay between transaction commit
// and message publishing, while still ensuring consistency if publishing fails.
func WithOptimisticPublisher(msgPublisher MessagePublisher) WriterOption {
	return func(w *Writer) {
		w.msgPublisher = msgPublisher
	}
}

// NewWriter creates a new outbox Writer with the given database connection and options.
func NewWriter(db *sql.DB, opts ...WriterOption) *Writer {
	w := &Writer{
		sqlExecutor: &coreSql.DBAdapter{DB: db},
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Write stores a message in the outbox table as part of a transaction, and executes the provided callback
// within the same transaction. This ensures that if the callback succeeds but storing the message
// fails, the entire transaction is rolled back.
//
// If optimistic publishing is enabled, the message will also be published to the external system
// after the transaction is committed asynchronously.
func (w *Writer) Write(ctx context.Context, msg Message, writerTxFunc WriterTxFunc) error {
	tx, err := w.sqlExecutor.BeginTx()
	if err != nil {
		return err
	}

	var txCommitted bool
	defer func() {
		if !txCommitted {
			_ = tx.Rollback()
		}
	}()

	err = writerTxFunc(ctx, tx.ExecContext)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO Outbox (id, created_at, context, payload) VALUES (%s, %s, %s, %s)",
		getSQLPlaceholder(1), getSQLPlaceholder(2), getSQLPlaceholder(3), getSQLPlaceholder(4))
	err = tx.ExecContext(ctx, query, msg.ID, msg.CreatedAt, msg.Context, msg.Payload)
	if err != nil {
		return err
	}

	err = tx.Commit()
	txCommitted = err == nil

	if txCommitted {
		ctxWithoutCancel := context.WithoutCancel(ctx)
		go w.publishMessage(ctxWithoutCancel, msg) // optimistically publish the message
	}

	return err
}

func (w *Writer) publishMessage(ctx context.Context, msg Message) {
	if w.msgPublisher == nil {
		return
	}

	err := w.msgPublisher.Publish(ctx, msg)
	if err == nil {
		query := fmt.Sprintf("DELETE FROM Outbox WHERE id = %s", getSQLPlaceholder(1))
		_ = w.sqlExecutor.ExecContext(ctx, query, msg.ID)
	}
}
