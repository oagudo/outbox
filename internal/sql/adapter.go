package sql

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

func (a *DBAdapter) BeginTx() (Tx, error) {
	tx, err := a.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &TxAdapter{tx}, nil
}

func (a *DBAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.DB.ExecContext(ctx, query, args...)
	return err
}
