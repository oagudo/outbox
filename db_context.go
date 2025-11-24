package outbox

import (
	"context"
	"database/sql"
	"fmt"
)

// SQLDialect represents a SQL database dialect.
type SQLDialect string

// Supported database dialects.
const (
	SQLDialectPostgres  SQLDialect = "postgres"
	SQLDialectMySQL     SQLDialect = "mysql"
	SQLDialectMariaDB   SQLDialect = "mariadb"
	SQLDialectSQLite    SQLDialect = "sqlite"
	SQLDialectOracle    SQLDialect = "oracle"
	SQLDialectSQLServer SQLDialect = "sqlserver"
)

// Queryer represents a query executor.
type Queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// TxQueryer represents a query executor inside a transaction.
type TxQueryer interface {
	Queryer
}

// Tx represents a database transaction.
// It is compatible with the standard sql.Tx type.
type Tx interface {
	Commit() error
	Rollback() error
	TxQueryer
}

// DB represents a database connection.
// It is compatible with the standard sql.DB type.
type DB interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	Queryer
}

// DBContext holds the database connection and the SQL dialect.
type DBContext struct {
	db      DB
	dialect SQLDialect
}

// NewDBContext creates a new DBContext from a standard *sql.DB.
func NewDBContext(db *sql.DB, dialect SQLDialect) *DBContext {
	return NewDBContextWithDB(&dbAdapter{DB: db}, dialect)
}

// NewDBContextWithDB creates a new DBContext with a custom DB implementation.
// This is useful for users who want to provide their own database abstraction or for testing.
func NewDBContextWithDB(db DB, dialect SQLDialect) *DBContext {
	return &DBContext{
		db:      db,
		dialect: dialect,
	}
}

// formatMessageIDForDB formats the message ID for the based on the SQL dialect.
func (c *DBContext) formatMessageIDForDB(msg *Message) any {
	switch c.dialect {
	case SQLDialectMySQL, SQLDialectOracle, SQLDialectSQLServer:
		bytes, _ := msg.ID.MarshalBinary() // Convert UUID to binary for better storage
		return bytes
	case SQLDialectPostgres, SQLDialectMariaDB:
		return msg.ID // Native support
	default:
		return msg.ID.String()
	}
}

// getSQLPlaceholder returns the appropriate SQL placeholder for the given index.
func (c *DBContext) getSQLPlaceholder(index int) string {
	switch c.dialect {
	case SQLDialectPostgres:
		return fmt.Sprintf("$%d", index)

	case SQLDialectOracle:
		return fmt.Sprintf(":%d", index)

	case SQLDialectSQLServer:
		return fmt.Sprintf("@p%d", index)

	default:
		return "?"
	}
}

func (c *DBContext) getCurrentTimestampInUTC() string {
	switch c.dialect {
	case SQLDialectPostgres:
		return "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'"
	case SQLDialectMySQL, SQLDialectMariaDB:
		return "UTC_TIMESTAMP()"
	case SQLDialectOracle:
		return "SYSTIMESTAMP AT TIME ZONE 'UTC'"
	case SQLDialectSQLServer:
		return "SYSUTCDATETIME()"
	default:
		return "CURRENT_TIMESTAMP"
	}
}

func (c *DBContext) buildSelectMessagesQuery() string {
	limitPlaceholder := c.getSQLPlaceholder(1)

	switch c.dialect {
	case SQLDialectOracle:
		return fmt.Sprintf(`SELECT id, payload, created_at, scheduled_at, metadata, times_attempted 
			FROM outbox
			WHERE scheduled_at <= %s
			ORDER BY created_at ASC FETCH FIRST %s ROWS ONLY`, c.getCurrentTimestampInUTC(), limitPlaceholder)

	case SQLDialectSQLServer:
		return fmt.Sprintf(`SELECT TOP (%s) id, payload, created_at, scheduled_at, metadata, times_attempted 
			FROM outbox
			WHERE scheduled_at <= %s
			ORDER BY created_at ASC`, limitPlaceholder, c.getCurrentTimestampInUTC())

	default:
		return fmt.Sprintf(`SELECT id, payload, created_at, scheduled_at, metadata, times_attempted 
			FROM outbox
			WHERE scheduled_at <= %s
			ORDER BY created_at ASC LIMIT %s`, c.getCurrentTimestampInUTC(), limitPlaceholder)
	}
}

// txAdapter is a wrapper around a sql.Tx that implements the Tx interface.
type txAdapter struct {
	tx *sql.Tx
}

func NewTxAdapter(tx *sql.Tx) *txAdapter {
	return &txAdapter{tx}
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
	return NewTxAdapter(tx), nil
}

func (a *dbAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.DB.ExecContext(ctx, query, args...)
}

func (a *dbAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.DB.QueryContext(ctx, query, args...)
}
