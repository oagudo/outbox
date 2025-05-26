package outbox

import (
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

// DBContext holds the database connection and the SQL dialect.
type DBContext struct {
	db      *sql.DB
	dialect SQLDialect
}

// NewDBContext creates a new DBContext.
func NewDBContext(db *sql.DB, dialect SQLDialect) *DBContext {
	return &DBContext{
		db:      db,
		dialect: dialect,
	}
}

// formatMessageIDForDB formats the message ID for the based on the SQL dialect.
func (c *DBContext) formatMessageIDForDB(msg Message) any {
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
