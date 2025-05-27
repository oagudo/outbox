package outbox

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MessagePublisher defines an interface for publishing messages to an external system.
type MessagePublisher interface {
	// Publish sends a message to an external system (like a message broker).
	// This function can be invoked multiple times for the same message.
	// Message consumers must be idempotent and not affected by receiving duplicate messages,
	// though some brokers also provide deduplication features.
	Publish(ctx context.Context, msg Message) error
}

// OpKind represents the type of operation that failed.
type OpKind uint8

const (
	OpRead    OpKind = iota // reading messages from the outbox
	OpPublish               // publishing messages to the external system
	OpDelete                // deleting messages from the outbox
)

// ReaderError represents an error that occurred during a reader operation.
type ReaderError struct {
	Op  OpKind
	Msg Message // zero value when Op == OpRead
	Err error
}

// Reader periodically reads unpublished messages from the outbox table
// and attempts to publish them to an external system.
type Reader struct {
	dbCtx        *DBContext
	msgPublisher MessagePublisher

	interval       time.Duration
	readTimeout    time.Duration
	publishTimeout time.Duration
	deleteTimeout  time.Duration
	maxMessages    int

	started int32
	closed  int32
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	errCh   chan ReaderError
}

// ReaderOption is a function that configures a Reader instance.
type ReaderOption func(*Reader)

// WithInterval sets the time between outbox reader processing attempts.
// Default is 10 seconds.
func WithInterval(interval time.Duration) ReaderOption {
	return func(r *Reader) {
		r.interval = interval
	}
}

// WithReadTimeout sets the timeout for reading messages from the outbox.
// Default is 5 seconds.
func WithReadTimeout(timeout time.Duration) ReaderOption {
	return func(r *Reader) {
		r.readTimeout = timeout
	}
}

// WithPublishTimeout sets the timeout for publishing messages to the external system.
// Default is 5 seconds.
func WithPublishTimeout(timeout time.Duration) ReaderOption {
	return func(r *Reader) {
		r.publishTimeout = timeout
	}
}

// WithDeleteTimeout sets the timeout for deleting messages from the outbox.
// Default is 5 seconds.
func WithDeleteTimeout(timeout time.Duration) ReaderOption {
	return func(r *Reader) {
		r.deleteTimeout = timeout
	}
}

// WithMaxMessages sets the maximum number of messages to process in a single batch.
// Default is 100 messages. Must be positive.
func WithMaxMessages(maxMessages int) ReaderOption {
	return func(r *Reader) {
		if maxMessages > 0 {
			r.maxMessages = maxMessages
		}
	}
}

// WithErrorChannelSize sets the size of the error channel.
// Default is 128. Size must be positive.
func WithErrorChannelSize(size int) ReaderOption {
	return func(r *Reader) {
		if size > 0 {
			r.errCh = make(chan ReaderError, size)
		}
	}
}

// NewReader creates a new outbox Reader with the given database context,
// message publisher, and options.
func NewReader(dbCtx *DBContext, msgPublisher MessagePublisher, opts ...ReaderOption) *Reader {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Reader{
		dbCtx:          dbCtx,
		msgPublisher:   msgPublisher,
		ctx:            ctx,
		cancel:         cancel,
		interval:       10 * time.Second,
		readTimeout:    5 * time.Second,
		publishTimeout: 5 * time.Second,
		deleteTimeout:  5 * time.Second,
		maxMessages:    100,
		errCh:          make(chan ReaderError, 128),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Start begins the background processing of outbox messages.
// It periodically reads unpublished messages and attempts to publish them.
// If Start is called multiple times, only the first call has an effect.
func (r *Reader) Start() {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return
	}

	r.wg.Add(1)
	go func() {
		ticker := time.NewTicker(r.interval)

		defer r.wg.Done()
		defer close(r.errCh)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.publishMessages()
			case <-r.ctx.Done():
				return
			}
		}
	}()
}

// Stop gracefully shuts down the outbox reader processing.
// It prevents new reader cycles from starting and waits for any ongoing
// message publishing to complete. The provided context controls how long to wait
// for graceful shutdown before giving up.
//
// If the context expires before processing completes, Stop returns the context's
// error. If shutdown completes successfully, it returns nil.
// Calling Stop multiple times is safe and only the first call has an effect.
func (r *Reader) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return nil
	}

	r.cancel() // signal stop

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Errors returns a channel that receives errors from the outbox reader.
// The channel is buffered to prevent blocking the reader. If the buffer becomes
// full, subsequent errors will be dropped to maintain reader throughput.
// The channel is closed when the reader is stopped.
//
// Consumers should drain this channel promptly to avoid missing errors.
func (r *Reader) Errors() <-chan ReaderError {
	return r.errCh
}

func (r *Reader) sendError(err ReaderError) {
	select {
	case r.errCh <- err:
	default:
		// Channel buffer full, drop the error to prevent blocking
	}
}

func (r *Reader) publishMessages() {
	msgs, err := r.readOutboxMessages()
	if err != nil {
		r.sendError(ReaderError{Op: OpRead, Err: err})
		return
	}

	for _, msg := range msgs {
		err := r.publishMessage(msg)
		if err != nil {
			r.sendError(ReaderError{Op: OpPublish, Msg: msg, Err: err})
			continue
		}

		err = r.deleteMessage(msg)
		if err != nil {
			r.sendError(ReaderError{Op: OpDelete, Msg: msg, Err: err})
		}
	}
}

func (r *Reader) publishMessage(msg Message) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.publishTimeout)
	defer cancel()

	return r.msgPublisher.Publish(ctx, msg)
}

func (r *Reader) deleteMessage(msg Message) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.deleteTimeout)
	defer cancel()

	// nolint:gosec
	query := fmt.Sprintf("DELETE FROM Outbox WHERE id = %s", r.dbCtx.getSQLPlaceholder(1))
	_, err := r.dbCtx.db.ExecContext(ctx, query, r.dbCtx.formatMessageIDForDB(msg))
	if err != nil {
		return fmt.Errorf("failed to delete message %s from outbox: %w", msg.ID, err)
	}

	return nil
}

func (r *Reader) readOutboxMessages() ([]Message, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.readTimeout)
	defer cancel()

	// nolint:gosec
	query := r.buildSelectMessagesQuery()
	rows, err := r.dbCtx.db.QueryContext(ctx, query, r.maxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to read outbox messages: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.Payload, &msg.CreatedAt, &msg.Context); err != nil {
			return nil, fmt.Errorf("failed to scan outbox message: %w", err)
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unexpected error while scanning outbox messages: %w", err)
	}
	return messages, nil
}

func (r *Reader) buildSelectMessagesQuery() string {
	limitPlaceholder := r.dbCtx.getSQLPlaceholder(1)

	switch r.dbCtx.dialect {
	case SQLDialectOracle:
		return fmt.Sprintf("SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC FETCH FIRST %s ROWS ONLY", limitPlaceholder)

	case SQLDialectSQLServer:
		return fmt.Sprintf("SELECT TOP (%s) id, payload, created_at, context FROM Outbox ORDER BY created_at ASC", limitPlaceholder)

	default:
		return fmt.Sprintf("SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT %s", limitPlaceholder)
	}
}
