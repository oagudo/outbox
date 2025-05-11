package outbox

import (
	"context"
	"database/sql"

	coreSql "github.com/oagudo/outbox/internal/sql"
)

type QueryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) error
}

type MessagePublisher interface {
	Publish(ctx context.Context, msg Message) error // TODO: Check if context is needed
}

type Writer struct {
	sqlExecutor  coreSql.Executor
	msgPublisher MessagePublisher
}

type WriterCallback func(ctx context.Context, tx QueryExecutor) error

type WriterOption func(*Writer)

func WithOptimisticPublisher(msgPublisher MessagePublisher) WriterOption {
	return func(w *Writer) {
		w.msgPublisher = msgPublisher
	}
}

func NewWriter(db *sql.DB, opts ...WriterOption) *Writer {
	w := &Writer{
		sqlExecutor: &coreSql.DBAdapter{DB: db},
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

func (w *Writer) Write(ctx context.Context, msg Message, callback WriterCallback) error {
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

	err = callback(ctx, tx)
	if err != nil {
		return err
	}

	err = tx.ExecContext(ctx, "INSERT INTO Outbox (id, created_at, context, payload) VALUES ($1, $2, $3, $4)",
		msg.ID, msg.CreatedAt, msg.Context, msg.Payload)
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
		_ = w.sqlExecutor.ExecContext(ctx, "DELETE FROM Outbox WHERE id = $1", msg.ID)
	}
}
