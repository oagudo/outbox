package sqladapter

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
	// ExecContextWithResult executes a query and returns the sql.Result for advanced use-cases (e.g., checking affected rows).
	ExecContextWithResult(ctx context.Context, query string, args ...any) (sql.Result, error)
}
