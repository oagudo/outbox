package sql

import "context"

type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) error
	Commit() error
	Rollback() error
}

type SqlExecutor interface {
	BeginTx() (Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) error
}
