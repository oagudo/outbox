package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	defaultInterval    = 10 * time.Second
	defaultMaxMessages = 100
)

// MessagePublisher defines an interface for publishing messages to an external system.
type MessagePublisher interface {
	// Publish sends a message to an external system (like a message broker).
	// This function can be invoked multiple times for the same message.
	// Message consumers must be idempotent and not affected by receiving duplicate messages.
	Publish(ctx context.Context, msg Message) error
}

// OnReadErrorCallback is a function called when an error occurs while reading messages from the outbox.
type OnReadErrorCallback func(error)

// OnMessageErrorCallback is a function called when an error occurs while deleting a message from the outbox
// after it has been successfully published.
type OnMessageErrorCallback func(Message, error)

// Reader periodically reads unpublished messages from the outbox table
// and attempts to publish them to an external system.
type Reader struct {
	db           *sql.DB
	msgPublisher MessagePublisher

	onDeleteErrorCallback OnMessageErrorCallback
	onReadErrorCallback   OnReadErrorCallback

	interval    time.Duration
	maxMessages int
	started     int32
	closed      int32
	stop        chan struct{}
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
func WithOnDeleteError(callback OnMessageErrorCallback) ReaderOption {
	return func(r *Reader) {
		r.onDeleteErrorCallback = callback
	}
}

// WithOnReadError sets a callback function that is called when an error occurs
// while reading messages from the outbox.
func WithOnReadError(callback OnReadErrorCallback) ReaderOption {
	return func(r *Reader) {
		r.onReadErrorCallback = callback
	}
}

func noOpMessageErrorCallback(Message, error) {}

func noOpReadErrorCallback(error) {}

// NewReader creates a new outbox Reader with the given database connection,
// message publisher, and options.
func NewReader(db *sql.DB, msgPublisher MessagePublisher, opts ...ReaderOption) *Reader {
	r := &Reader{
		db:           db,
		msgPublisher: msgPublisher,
		stop:         make(chan struct{}),

		interval:    defaultInterval,
		maxMessages: defaultMaxMessages,

		onDeleteErrorCallback: noOpMessageErrorCallback,
		onReadErrorCallback:   noOpReadErrorCallback,
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

	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.publishMessages()
			case <-r.stop:
				return
			}
		}
	}()
}

// Stop halts the background processing of outbox messages.
// If Stop is called multiple times, only the first call has an effect.
func (r *Reader) Stop() {
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return
	}

	close(r.stop)
}

func (r *Reader) publishMessages() {
	msgs, err := readOutboxMessages(r.db, r.maxMessages)
	if err != nil {
		r.onReadErrorCallback(err)
		return
	}

	for _, msg := range msgs {
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
