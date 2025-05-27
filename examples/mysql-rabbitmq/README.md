# MySQL-RabbitMQ Outbox Example

This example demonstrates the usage of the outbox library using MySQL and RabbitMQ.

## Getting Started

1. Start dependencies and run the service:
```bash
../../scripts/up-and-wait.sh && go run service.go
```

2. In another terminal, trigger the following request to create an entity:
```bash
curl -X POST http://localhost:8080/entity
```

This will:
1. Store both the entity and outbox message to MySQL in a single transaction via the outbox writer
2. Publish the message to RabbitMQ queue asynchronously via the outbox reader