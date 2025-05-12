module posgres-kafka-example

go 1.22.2

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.5.4
	github.com/segmentio/kafka-go v0.4.45
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/oagudo/outbox v0.0.0-20250512074227-c5f225433abc
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/oagudo/outbox => ../../
