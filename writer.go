package outbox

import (
	"context"
	"fmt"
	"time"
)

// Writer handles storing messages in the outbox table as part of user-defined queries within a database transaction.
// It optionally supports optimistic publishing, which attempts to publish messages
// immediately after transaction commit.
type Writer struct {
	dbCtx        *DBContext
	msgPublisher MessagePublisher

	optimisticTimeout time.Duration
}

// TxWorkFunc is user-supplied callback that accepts a TxQueryer parameter
// that executes user-defined queries within the transaction that stores a message in the outbox.
// The Writer itself commits or rolls back the transaction once the callback and the outbox insert complete.
type TxWorkFunc func(ctx context.Context, txQueryer TxQueryer) error

// WriterOption is a function that configures a Writer instance.
type WriterOption func(*Writer)

// WithOptimisticPublisher configures the Writer to attempt immediate publishing
// of messages after the transaction is committed.
// This can improve performance by reducing the delay between transaction commit
// and message publishing, while still ensuring consistency if publishing fails.
//
// Note: optimistic path is just an efficiency optimization, not something the system
// depends on for correctness. If the message is not published, it will be retried by the reader.
// Due to this retry mechanism, duplicate message deliveries may occur
// (e.g. reader wakes up just after message is committed).
func WithOptimisticPublisher(msgPublisher MessagePublisher) WriterOption {
	return func(w *Writer) {
		w.msgPublisher = msgPublisher
	}
}

// WithOptimisticTimeout sets the timeout for optimistic publishing and deleting messages.
// Default is 10 seconds.
func WithOptimisticTimeout(timeout time.Duration) WriterOption {
	return func(w *Writer) {
		w.optimisticTimeout = timeout
	}
}

// NewWriter creates a new outbox Writer with the given database context and options.
func NewWriter(dbCtx *DBContext, opts ...WriterOption) *Writer {
	w := &Writer{
		dbCtx:             dbCtx,
		optimisticTimeout: 10 * time.Second,
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
func (w *Writer) Write(ctx context.Context, msg *Message, txWorkFunc TxWorkFunc) error {
	tx, err := w.dbCtx.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	var txCommitted bool
	defer func() {
		if !txCommitted {
			_ = tx.Rollback()
		}
	}()

	err = txWorkFunc(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to execute user-defined query: %w", err)
	}

	query := fmt.Sprintf("INSERT INTO outbox (id, created_at, scheduled_at, metadata, payload, times_attempted) VALUES (%s, %s, %s, %s, %s, %s)",
		w.dbCtx.getSQLPlaceholder(1),
		w.dbCtx.getSQLPlaceholder(2),
		w.dbCtx.getSQLPlaceholder(3),
		w.dbCtx.getSQLPlaceholder(4),
		w.dbCtx.getSQLPlaceholder(5),
		w.dbCtx.getSQLPlaceholder(6))
	_, err = tx.ExecContext(ctx, query, w.dbCtx.formatMessageIDForDB(msg), msg.CreatedAt, msg.ScheduledAt, msg.Metadata, msg.Payload, 0)
	if err != nil {
		return fmt.Errorf("failed to store message in outbox: %w", err)
	}

	err = tx.Commit()
	txCommitted = err == nil

	if txCommitted && w.msgPublisher != nil {
		ctxWithoutCancel := context.WithoutCancel(ctx) // optimistic path is async, so we don't want to cancel the context
		go w.publishMessage(ctxWithoutCancel, msg)
	}

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (w *Writer) publishMessage(ctx context.Context, msg *Message) {
	ctx, cancel := context.WithTimeout(ctx, w.optimisticTimeout)
	defer cancel()

	err := w.msgPublisher.Publish(ctx, msg)
	if err == nil {
		query := fmt.Sprintf("DELETE FROM outbox WHERE id = %s", w.dbCtx.getSQLPlaceholder(1))
		_, _ = w.dbCtx.db.ExecContext(ctx, query, w.dbCtx.formatMessageIDForDB(msg))
	}
}
