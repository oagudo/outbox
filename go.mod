module github.com/oagudo/outbox

go 1.22.2

require github.com/google/uuid v1.6.0

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// test dependencies - wont be required when using the library
require (
	github.com/lib/pq v1.10.9
	github.com/stretchr/testify v1.10.0
)
