module outbox-integration-test

go 1.24.3

require (
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/go-sql-driver/mysql v1.9.2
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.28
	github.com/oagudo/outbox v0.4.0
	github.com/sijms/go-ora/v2 v2.8.24
	github.com/stretchr/testify v1.10.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/oagudo/outbox => ../
