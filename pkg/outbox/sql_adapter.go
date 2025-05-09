package outbox

import (
	"context"
	"database/sql"
)

type sqlTxAdapter struct {
	tx *sql.Tx
}

func (a *sqlTxAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.tx.ExecContext(ctx, query, args...)
	return err
}

func (a *sqlTxAdapter) Commit() error {
	return a.tx.Commit()
}

func (a *sqlTxAdapter) Rollback() error {
	return a.tx.Rollback()
}

type sqlAdapter struct {
	db *sql.DB
}

func (a *sqlAdapter) BeginTx() (Tx, error) {
	tx, err := a.db.Begin()
	if err != nil {
		return nil, err
	}
	return &sqlTxAdapter{tx}, nil
}

func (a *sqlAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.db.ExecContext(ctx, query, args...)
	return err
}
