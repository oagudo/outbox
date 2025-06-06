package sqladapter

import (
	"context"
	"database/sql"
)

type TxAdapter struct {
	tx *sql.Tx
}

func (a *TxAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.tx.ExecContext(ctx, query, args...)
}

func (a *TxAdapter) Commit() error {
	return a.tx.Commit()
}

func (a *TxAdapter) Rollback() error {
	return a.tx.Rollback()
}

type DBAdapter struct {
	DB *sql.DB
}

func (a *DBAdapter) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &TxAdapter{tx}, nil
}

func (a *DBAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.DB.ExecContext(ctx, query, args...)
	return err
}

// ExecContextWithResult executes a query and returns the sql.Result for advanced use-cases (e.g., checking affected rows).
func (a *DBAdapter) ExecContextWithResult(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.DB.ExecContext(ctx, query, args...)
}
