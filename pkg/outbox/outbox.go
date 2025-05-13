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

// DriverType represents a SQL database type.
type DriverType string

// Supported database driver types.
const (
	DriverPostgres  DriverType = "postgres"
	DriverMySQL     DriverType = "mysql"
	DriverMariaDB   DriverType = "mariadb"
	DriverSQLite    DriverType = "sqlite"
	DriverOracle    DriverType = "oracle"
	DriverSQLServer DriverType = "sqlserver"
)

var o *outbox

type outbox struct {
	dbDriver DriverType
}

// SetDriver sets the SQL driver used by the outbox.
// This configuration affects how SQL placeholders are generated in queries.
// If not set, PostgreSQL is used as the default driver.
func SetDriver(driver DriverType) {
	o.setDriver(driver)
}

func init() {
	o = newOutbox()
}

func newOutbox() *outbox {
	return &outbox{
		dbDriver: DriverPostgres,
	}
}

func (o *outbox) setDriver(driver DriverType) {
	o.dbDriver = driver
}

func getSQLPlaceholder(index int) string {
	switch o.dbDriver {
	case DriverPostgres:
		return fmt.Sprintf("$%d", index)

	case DriverMariaDB, DriverMySQL, DriverSQLite:
		return "?"

	case DriverOracle:
		return fmt.Sprintf(":%d", index)

	case DriverSQLServer:
		return fmt.Sprintf("@p%d", index)

	default:
		return "?"
	}
}
