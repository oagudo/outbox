package outbox

import (
	"context"
	"database/sql"
)

type TxProvider interface {
	Begin() (*sql.Tx, error)
}

type MessagePublisher interface {
	Publish(ctx context.Context, msg Message) error
}

type Writer struct {
	txProvider   TxProvider
	msgPublisher MessagePublisher
}

type WriterCallback func(ctx context.Context, tx *sql.Tx) error

type WriterOption func(*Writer)

func WithOptimisticPublisher(msgPublisher MessagePublisher) WriterOption {
	return func(w *Writer) {
		w.msgPublisher = msgPublisher
	}
}

func NewWriter(txProvider TxProvider, opts ...WriterOption) *Writer {
	w := &Writer{
		txProvider: txProvider,
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

func (w *Writer) Write(ctx context.Context, msg Message, cb WriterCallback) error {
	tx, err := w.txProvider.Begin()
	if err != nil {
		return err
	}

	var txCommitted bool
	defer func() {
		if !txCommitted {
			_ = tx.Rollback()
		}
	}()

	err = cb(ctx, tx)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO Outbox (id, created_at, context, payload) VALUES ($1, $2, $3, $4)",
		msg.ID, msg.CreatedAt, msg.Context, msg.Payload)
	if err != nil {
		return err
	}

	err = tx.Commit()
	txCommitted = err == nil

	if txCommitted {
		go w.publishMessage(ctx, msg) // optimistically publish the message
	}

	return err
}

func (w *Writer) publishMessage(ctx context.Context, msg Message) {
	if w.msgPublisher == nil {
		return
	}

	ctxWithoutCancel := context.WithoutCancel(ctx)
	w.msgPublisher.Publish(ctxWithoutCancel, msg)
}
