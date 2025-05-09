package sql

import (
	"context"
	"database/sql"
)

type SqlTxAdapter struct {
	tx *sql.Tx
}

func (a *SqlTxAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.tx.ExecContext(ctx, query, args...)
	return err
}

func (a *SqlTxAdapter) Commit() error {
	return a.tx.Commit()
}

func (a *SqlTxAdapter) Rollback() error {
	return a.tx.Rollback()
}

type SqlAdapter struct {
	DB *sql.DB
}

func (a *SqlAdapter) BeginTx() (Tx, error) {
	tx, err := a.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &SqlTxAdapter{tx}, nil
}

func (a *SqlAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.DB.ExecContext(ctx, query, args...)
	return err
}
