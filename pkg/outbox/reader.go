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

	interval    time.Duration
	maxMessages int

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

		interval:    10 * time.Second,
		maxMessages: 100,

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
	msgs, err := readOutboxMessages(r.db, r.maxMessages)
	if err != nil {
		r.onReadErrorCallback(err)
		return
	}

	for _, msg := range msgs {
		if r.ctx.Err() != nil {
			return
		}

		err := r.msgPublisher.Publish(context.Background(), msg)
		if err != nil {
			continue
		}

		// nolint:gosec
		query := fmt.Sprintf("DELETE FROM Outbox WHERE id = %s", getSQLPlaceholder(1))
		_, err = r.db.Exec(query, msg.ID)
		if err != nil {
			r.onDeleteErrorCallback(msg, err)
		}
	}
}

func readOutboxMessages(db *sql.DB, limit int) ([]Message, error) {
	// nolint:gosec
	query := fmt.Sprintf("SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT %s", getSQLPlaceholder(1))
	rows, err := db.Query(query, limit)
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
