package sql

import (
	"context"
	"database/sql"
)

type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

type Executor interface {
	BeginTx(ctx context.Context) (Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) error
}
