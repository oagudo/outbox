module posgres-kafka-example

go 1.24.3

require (
	github.com/go-sql-driver/mysql v1.7.1
	github.com/google/uuid v1.6.0
	github.com/rabbitmq/amqp091-go v1.9.0
)

require github.com/oagudo/outbox v0.0.0-20250512092004-3a273cf600f3

replace github.com/oagudo/outbox => ../../
