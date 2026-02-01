package outbox

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// Writer handles storing messages in the outbox table as part of user-defined queries within a database transaction.
// It optionally supports optimistic publishing, which attempts to publish messages
// immediately after transaction commit.
type Writer struct {
	dbCtx           *DBContext
	msgPublisher    MessagePublisher
	unmanagedWriter *UnmanagedWriter

	optimisticTimeout time.Duration
}

// UnmanagedWriter provides low-level access to outbox table persistence
//
// Unlike Writer, UnmanagedWriter does not start, commit, or rollback
// transactions, nor does support optimistic publishing.
// It is intended for users who want to manage the transaction lifecycle
// themselves and only need to persist outbox messages.
//
// An UnmanagedWriter must be obtained via Writer.Unmanaged() function.
type UnmanagedWriter struct {
	dbCtx *DBContext
}

// TxWorkFunc is the user supplied callback for [Writer.WriteOne].
// It executes user defined queries within the same transaction that stores the given outbox message.
// The Writer commits or rolls back the transaction once the callback completes.
type TxWorkFunc func(ctx context.Context, tx TxQueryer) error

// OutboxWorkFunc is the user supplied callback for [Writer.Write].
// It executes user defined queries and stores messages in the outbox table within the same transaction.
// The Writer commits or rolls back the transaction once the callback completes.
type OutboxWorkFunc func(ctx context.Context, tx TxQueryer, msgWriter MessageWriter) error

// MessageWriter allows storing messages within a managed transaction.
type MessageWriter interface {
	// Store persists a message in the outbox table.
	// The message is committed when the enclosing transaction commits.
	Store(ctx context.Context, msg *Message) error
}

// WriterOption is a function that configures a Writer instance.
type WriterOption func(*Writer)

// WithOptimisticPublisher configures the Writer to attempt immediate publishing
// of messages after the transaction is committed.
// This can improve performance by reducing the delay between transaction commit
// and message publishing, while still ensuring consistency if publishing fails.
//
// Messages are published sequentially in CreatedAt order. If a publish fails,
// remaining messages are left for the Reader to handle.
//
// Note: optimistic path is just an efficiency optimization, not something the system
// depends on for correctness. If the message is not published, it will be retried by the reader.
// Due to this retry mechanism, duplicate message deliveries may occur
// (e.g. reader wakes up just after message is committed and publishes it again).
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
		unmanagedWriter:   &UnmanagedWriter{dbCtx: dbCtx},
		optimisticTimeout: 10 * time.Second,
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Write executes user defined queries and stores messages in the outbox table within the same managed transaction.
//
// This is the recommended approach when you need to:
//   - Conditionally store messages based on business logic
//   - Store multiple messages per transaction
//
// The transaction commits if the callback returns nil, or rolls back if it
// returns an error or panics. Messages are committed atomically with your database changes.
//
// If optimistic publishing is configured, committed messages are published
// asynchronously after the transaction commits.
//
// Example:
//
//	err := writer.Write(ctx, func(ctx context.Context, tx outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
//	    // Perform business logic
//	    result, err := tx.ExecContext(ctx,
//	        "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2 AND quantity >= $1",
//	        order.Quantity, order.ProductID)
//	    if err != nil {
//	        return err
//	    }
//
//	    // Conditionally emit based on result
//	    rows, _ := result.RowsAffected()
//	    if rows == 0 {
//	        return ErrInsufficientInventory // no message emitted, transaction rolled back
//	    }
//
//	    return msgWriter.Store(ctx, outbox.NewMessage(orderPayload))
//	})
func (w *Writer) Write(ctx context.Context, fn OutboxWorkFunc) error {
	tx, err := w.dbCtx.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	var txCommitted bool
	defer func() {
		if !txCommitted {
			_ = tx.Rollback()
		}
	}()

	msgWriter := &messageWriter{
		dbCtx: w.dbCtx,
		tx:    tx,
	}

	err = fn(ctx, tx, msgWriter)
	if err != nil {
		return err
	}

	err = tx.Commit()
	txCommitted = err == nil

	if txCommitted && w.msgPublisher != nil {
		asyncCtx := context.WithoutCancel(ctx) // optimistic path is async, we don't want to cancel the context
		now := time.Now().UTC()                // freeze time for consistent scheduling decisions

		// Sort by CreatedAt to match Reader ordering (ORDER BY created_at ASC)
		slices.SortFunc(msgWriter.msgs, func(a, b *Message) int {
			return a.CreatedAt.Compare(b.CreatedAt)
		})

		go func() {
			for _, msg := range msgWriter.msgs {
				if !w.tryPublishMessage(asyncCtx, msg, now) {
					// Stop on first error - Reader will handle remaining messages
					return
				}
			}
		}()
	}

	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// WriteOne executes the provided callback and stores a message in the outbox table
// as part of a managed transaction.
//
// The transaction commits if the callback returns nil, or rolls back if it returns an error or
// panics.
//
// If optimistic publishing is configured, the message will also be published asynchronously to
// the external system after the transaction is committed.
//
// For conditional or multiple messages publishing use [Writer.Write] instead.
func (w *Writer) WriteOne(ctx context.Context, msg *Message, fn TxWorkFunc) error {
	return w.Write(ctx, func(ctx context.Context, tx TxQueryer, msgWriter MessageWriter) error {
		err := fn(ctx, tx)
		if err != nil {
			return err
		}

		return msgWriter.Store(ctx, msg)
	})
}

// Unmanaged returns an UnmanagedWriter that does not manage the transaction lifecycle.
// Useful for users who want to manage the transaction lifecycle themselves.
// Messages stored via Unmanaged are eventually published by the Reader but not by the optimistic publisher (if configured in Writer).
//
// Use [Writer.Write] for managed transaction lifecycle and optimistic publishing.
func (w *Writer) Unmanaged() *UnmanagedWriter {
	return w.unmanagedWriter
}

// Store persists a message into the outbox table using a user provided transaction.
//
// Store only writes the message if the provided transaction is committed successfully. It does not:
//   - manage the transaction lifecycle, it is the responsibility of the user to commit or rollback the transaction
//   - trigger optimistic publishing (if configured in Writer)
//
// Use Writer.Write for managed transaction lifecycle and optimistic publishing.
func (w *UnmanagedWriter) Store(ctx context.Context, tx TxQueryer, msg *Message) error {
	return insertOutboxMessage(ctx, w.dbCtx, tx, msg)
}

type messageWriter struct {
	dbCtx *DBContext
	tx    TxQueryer
	msgs  []*Message
}

func (w *messageWriter) Store(ctx context.Context, msg *Message) error {
	err := insertOutboxMessage(ctx, w.dbCtx, w.tx, msg)
	if err != nil {
		return err
	}
	w.msgs = append(w.msgs, msg)
	return nil
}

// tryPublishMessage attempts to publish a message and delete it from the outbox.
// Returns true if the message was successfully published or skipped (scheduled for later),
// false if publishing failed.
// On failure, the message remains in the outbox for the Reader to handle.
// The now parameter ensures consistent scheduling decisions across all messages in a batch.
func (w *Writer) tryPublishMessage(ctx context.Context, msg *Message, now time.Time) bool {
	// Skip messages scheduled for the future - let the Reader handle them at the right time
	if msg.ScheduledAt.After(now) {
		return true
	}

	ctx, cancel := context.WithTimeout(ctx, w.optimisticTimeout)
	defer cancel()

	err := w.msgPublisher.Publish(ctx, msg)
	if err != nil {
		return false
	}

	query := fmt.Sprintf("DELETE FROM outbox WHERE id = %s", w.dbCtx.getSQLPlaceholder(1))
	_, _ = w.dbCtx.db.ExecContext(ctx, query, w.dbCtx.formatMessageIDForDB(msg))
	return true
}

func insertOutboxMessage(ctx context.Context, dbCtx *DBContext, tx TxQueryer, msg *Message) error {
	query := fmt.Sprintf("INSERT INTO outbox (id, created_at, scheduled_at, metadata, payload, times_attempted) VALUES (%s, %s, %s, %s, %s, %s)",
		dbCtx.getSQLPlaceholder(1),
		dbCtx.getSQLPlaceholder(2),
		dbCtx.getSQLPlaceholder(3),
		dbCtx.getSQLPlaceholder(4),
		dbCtx.getSQLPlaceholder(5),
		dbCtx.getSQLPlaceholder(6))
	_, err := tx.ExecContext(ctx, query, dbCtx.formatMessageIDForDB(msg), msg.CreatedAt, msg.ScheduledAt, msg.Metadata, msg.Payload, 0)
	if err != nil {
		return fmt.Errorf("storing message in outbox: %w", err)
	}
	return nil
}
