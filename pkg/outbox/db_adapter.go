package outbox

import (
	"context"
	"database/sql"
)

// NewDB creates a new DB from a standard sql.DB.
func NewDB(db *sql.DB) DB {
	return &dbAdapter{DB: db}
}

// txAdapter is a wrapper around a sql.Tx that implements the Tx interface.
type txAdapter struct {
	tx *sql.Tx
}

func (a *txAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.tx.ExecContext(ctx, query, args...)
}

func (a *txAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.tx.QueryContext(ctx, query, args...)
}

func (a *txAdapter) Commit() error {
	return a.tx.Commit()
}

func (a *txAdapter) Rollback() error {
	return a.tx.Rollback()
}

// dbAdapter is a wrapper around a sql.DB that implements the DB interface.
type dbAdapter struct {
	DB *sql.DB
}

func (a *dbAdapter) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := a.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &txAdapter{tx}, nil
}

func (a *dbAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.DB.ExecContext(ctx, query, args...)
}

func (a *dbAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.DB.QueryContext(ctx, query, args...)
}
