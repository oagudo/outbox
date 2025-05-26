// Package outbox implements the transactional outbox pattern for reliable message publishing.
//
// The outbox pattern is a way to ensure that a message is reliably sent to a message broker
// even if the broker is temporarily unavailable. This is achieved by first storing messages
// in a database as part of your application's transaction, and then having a separate process
// that reads from this "outbox" and publishes the messages to the broker.
//
// This package provides:
// - A Writer to store messages in the database as part of a transaction
// - A Reader to retrieve and process unpublished messages
package outbox

import "fmt"

// SQLDialect represents a SQL database type.
type SQLDialect string

// Supported database dialects.
const (
	PostgresDialect  SQLDialect = "postgres"
	MySQLDialect     SQLDialect = "mysql"
	MariaDBDialect   SQLDialect = "mariadb"
	SQLiteDialect    SQLDialect = "sqlite"
	OracleDialect    SQLDialect = "oracle"
	SQLServerDialect SQLDialect = "sqlserver"
)

var o *outbox

type outbox struct {
	dbDialect SQLDialect
}

// SetSQLDialect sets the SQL dialect used by the outbox.
// This configuration affects how SQL queries are generated.
// If not set, PostgreSQL is used as the default dialect.
func SetSQLDialect(dialect SQLDialect) {
	o.setDialect(dialect)
}

func init() {
	o = newOutbox()
}

func newOutbox() *outbox {
	return &outbox{
		dbDialect: PostgresDialect,
	}
}

func (o *outbox) setDialect(dialect SQLDialect) {
	o.dbDialect = dialect
}

func formatMessageIDForDB(msg Message) any {
	switch o.dbDialect {
	case MySQLDialect, OracleDialect, SQLServerDialect:
		bytes, _ := msg.ID.MarshalBinary() // Convert UUID to binary for better storage
		return bytes
	case PostgresDialect, MariaDBDialect:
		return msg.ID // Native support
	default:
		return msg.ID.String()
	}
}

func getSQLPlaceholder(index int) string {
	switch o.dbDialect {
	case PostgresDialect:
		return fmt.Sprintf("$%d", index)

	case OracleDialect:
		return fmt.Sprintf(":%d", index)

	case SQLServerDialect:
		return fmt.Sprintf("@p%d", index)

	default:
		return "?"
	}
}
