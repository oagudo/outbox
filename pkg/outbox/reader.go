package outbox

import (
	"database/sql"
	"sync/atomic"
	"time"

	coreSql "github.com/oagudo/outbox/internal/sql"
)

type Reader struct {
	sqlExecutor  coreSql.SqlExecutor
	msgPublisher MessagePublisher

	interval    time.Duration
	maxMessages int

	started int32
	closed  int32
	stop    chan struct{}
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

func NewReader(db *sql.DB, msgPublisher MessagePublisher, opts ...ReaderOption) *Reader {
	r := &Reader{
		sqlExecutor:  &coreSql.SqlAdapter{DB: db},
		msgPublisher: msgPublisher,
		stop:         make(chan struct{}),
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

}
