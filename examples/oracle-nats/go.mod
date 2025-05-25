module oracle-nats-example

go 1.22.2

require (
	github.com/google/uuid v1.6.0
	github.com/nats-io/nats.go v1.31.0
)

require (
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/nats-io/nkeys v0.4.5 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/oagudo/outbox v0.0.0-20250512074227-c5f225433abc
	github.com/sijms/go-ora/v2 v2.8.24
	golang.org/x/crypto v0.6.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
)

replace github.com/oagudo/outbox => ../../
