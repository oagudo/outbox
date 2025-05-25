package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MessagePublisher defines an interface for publishing messages to an external system.
type MessagePublisher interface {
	// Publish sends a message to an external system (like a message broker).
	// This function can be invoked multiple times for the same message.
	// Message consumers must be idempotent and not affected by receiving duplicate messages.
	Publish(ctx context.Context, msg Message) error
}

// OnReadErrorFunc is a function called when an error occurs while reading messages from the outbox.
type OnReadErrorFunc func(error)

// OnMessageErrorFunc is a function called when an error occurs while deleting a message from the outbox
// after it has been successfully published.
type OnMessageErrorFunc func(Message, error)

// Reader periodically reads unpublished messages from the outbox table
// and attempts to publish them to an external system.
type Reader struct {
	db           *sql.DB
	msgPublisher MessagePublisher

	onDeleteErrorCallback OnMessageErrorFunc
	onReadErrorCallback   OnReadErrorFunc

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
// Default is 100 messages.
func WithMaxMessages(maxMessages int) ReaderOption {
	return func(r *Reader) {
		r.maxMessages = maxMessages
	}
}

// WithOnDeleteError sets a callback function that is called when an error occurs
// while deleting a message from the outbox after it has been successfully published.
func WithOnDeleteError(callback OnMessageErrorFunc) ReaderOption {
	return func(r *Reader) {
		r.onDeleteErrorCallback = callback
	}
}

// WithOnReadError sets a callback function that is called when an error occurs
// while reading messages from the outbox.
func WithOnReadError(callback OnReadErrorFunc) ReaderOption {
	return func(r *Reader) {
		r.onReadErrorCallback = callback
	}
}

func noOpMessageErrorFunc(Message, error) {}

func noOpReadErrorFunc(error) {}

// NewReader creates a new outbox Reader with the given database connection,
// message publisher, and options.
func NewReader(db *sql.DB, msgPublisher MessagePublisher, opts ...ReaderOption) *Reader {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Reader{
		db:           db,
		msgPublisher: msgPublisher,
		ctx:          ctx,
		cancel:       cancel,

		interval:       10 * time.Second,
		readTimeout:    5 * time.Second,
		publishTimeout: 5 * time.Second,
		deleteTimeout:  5 * time.Second,
		maxMessages:    100,

		onDeleteErrorCallback: noOpMessageErrorFunc,
		onReadErrorCallback:   noOpReadErrorFunc,
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
		defer ticker.Stop()
		defer r.wg.Done()

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

func (r *Reader) publishMessages() {
	msgs, err := r.readOutboxMessages()
	if err != nil {
		r.onReadErrorCallback(err)
		return
	}

	for _, msg := range msgs {
		err := r.publishMessage(msg)
		if err != nil {
			continue
		}

		err = r.deleteMessage(msg)
		if err != nil {
			r.onDeleteErrorCallback(msg, err)
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
	query := fmt.Sprintf("DELETE FROM Outbox WHERE id = %s", getSQLPlaceholder(1))
	_, err := r.db.ExecContext(ctx, query, formatMessageIDForDB(msg))
	return err
}

func (r *Reader) readOutboxMessages() ([]Message, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.readTimeout)
	defer cancel()

	// nolint:gosec
	query := buildSelectMessagesQuery()
	rows, err := r.db.QueryContext(ctx, query, r.maxMessages)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.Payload, &msg.CreatedAt, &msg.Context); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func buildSelectMessagesQuery() string {
	limitPlaceholder := getSQLPlaceholder(1)

	switch o.dbDriver {
	case DriverOracle:
		return fmt.Sprintf("SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC FETCH FIRST %s ROWS ONLY", limitPlaceholder)

	default:
		return fmt.Sprintf("SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT %s", limitPlaceholder)
	}
}
