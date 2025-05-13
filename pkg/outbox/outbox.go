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
	case "postgres", "postgresql", "pgsql":
		return fmt.Sprintf("$%d", index)

	case "mysql", "mariadb", "sqlite":
		return "?"

	case "oracle", "oci", "oci8":
		return fmt.Sprintf(":%d", index)

	case "sqlserver", "mssql":
		return fmt.Sprintf("@p%d", index)

	default:
		return "?" // Default to question mark as it's most common
	}
}
