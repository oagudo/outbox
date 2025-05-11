package outbox

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"
)

const (
	defaultInterval    = 10 * time.Second
	defaultMaxMessages = 100
)

type OnReadErrorCallback func(error)

type OnMessageErrorCallback func(Message, error)

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

type ReaderOption func(*Reader)

func WithInterval(interval time.Duration) ReaderOption {
	return func(r *Reader) {
		r.interval = interval
	}
}

func WithMaxMessages(maxMessages int) ReaderOption {
	return func(r *Reader) {
		r.maxMessages = maxMessages
	}
}

func WithOnDeleteError(callback OnMessageErrorCallback) ReaderOption {
	return func(r *Reader) {
		r.onDeleteErrorCallback = callback
	}
}

func WithOnReadError(callback OnReadErrorCallback) ReaderOption {
	return func(r *Reader) {
		r.onReadErrorCallback = callback
	}
}

func noOpMessageErrorCallback(Message, error) {}
func noOpReadErrorCallback(error)             {}

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

		_, err = r.db.Exec(`
			DELETE FROM Outbox
			WHERE id = $1
		`, msg.ID)
		if err != nil {
			r.onDeleteErrorCallback(msg, err)
		}
	}
}

func readOutboxMessages(db *sql.DB, limit int) ([]Message, error) {
	rows, err := db.Query(`
        SELECT id, payload, created_at
        FROM Outbox
        ORDER BY created_at ASC
        LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.Payload, &msg.CreatedAt); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}
