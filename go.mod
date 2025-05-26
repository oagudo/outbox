module github.com/oagudo/outbox

go 1.22.2

require github.com/google/uuid v1.6.0

// test dependencies - wont be required when using the library
require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-sql-driver/mysql v1.9.2
	github.com/lib/pq v1.10.9
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sijms/go-ora/v2 v2.8.24
	github.com/stretchr/testify v1.10.0
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
