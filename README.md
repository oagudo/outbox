# Outbox

[![Release](https://img.shields.io/github/release/oagudo/outbox.svg?style=flat-square)](https://github.com/oagudo/outbox/releases/latest)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE.md)
![GitHub Actions](https://github.com/oagudo/outbox/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/oagudo/outbox/graph/badge.svg?token=KH1GUAV4VR)](https://codecov.io/gh/oagudo/outbox)
[![Go Report Card](https://goreportcard.com/badge/github.com/oagudo/outbox?style=flat-square)](https://goreportcard.com/report/github.com/oagudo/outbox)
[![Go Reference](https://pkg.go.dev/badge/github.com/oagudo/outbox/v4.svg)](https://pkg.go.dev/github.com/oagudo/outbox)

Lightweight library for the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) in Go, not tied to any specific relational database or broker.

## Key Features

- **Minimal External Dependencies:** Doesn't impose additional dependencies (like specific Kafka, MySQL, etc. libraries) other than `google/uuid` on users of this library.
- **Database Agnostic:** Designed to work with PostgreSQL, MySQL, CockroachDB, and other relational databases.
- **Message Broker Agnostic:** Integrates seamlessly with popular brokers like Kafka, NATS, RabbitMQ, and others.
- **Simplicity:** Minimal, easy-to-understand codebase focused on core outbox pattern concepts.
- **Extensible:** Designed for easy customization and integration into your own projects.

## Usage

The library consists of two main components:

1. **Writer**: Stores your entity and corresponding message atomically within a transaction
2. **Reader**: Publishes stored messages to your message broker in the background

### The Writer

The Writer ensures your entity and outbox message are stored together atomically:

```go
// Setup database connection
db, _ := sql.Open("pgx", "postgres://user:password@localhost:5432/outbox?sslmode=disable")

// Create a writer instance
writer := outbox.NewWriter(db)

// Optional: Enable optimistic publishing to attempt publishing messages
// right after transaction commit (in addition to reader background process)
// writer := outbox.NewWriter(db, outbox.WithOptimisticPublisher(msgPublisher))

// In your business logic:
entity := Entity{
    ID:        uuid.New(),
    CreatedAt: time.Now().UTC(),
}

// Create outbox message
entityJSON, _ := json.Marshal(entity)
msg := outbox.Message{
    ID:        entity.ID,
    CreatedAt: entity.CreatedAt,
    Payload:   entityJSON,
    Context:   json.RawMessage(`{"trace_id":"abc123","correlation_id":"xyz789"}`), // Any additional metadata for the message
}

// Write message and entity in a single transaction
err = writer.Write(ctx, msg, func(ctx context.Context, txExecFunc outbox.TxExecFunc) error {
    return txExecFunc(ctx, // This query executes within a transaction
        "INSERT INTO Entity (id, created_at) VALUES (?, ?)",
        entity.ID.String(), entity.CreatedAt,
    )
})
```

### The Reader

The Reader periodically checks for unsent messages and publishes them to your message broker:

```go
// Create a message publisher implementation
type messagePublisher struct {
    // Your message broker client (e.g., Kafka, RabbitMQ)
}
func (p *messagePublisher) Publish(ctx context.Context, msg outbox.Message) error {
    // Publish the message to your broker. See examples below for specific implementations
    return nil
}

// Create and start the reader
reader := outbox.NewReader(
    db,                           // Same database connection
    &messagePublisher{},          // Your publisher implementation
    outbox.WithInterval(5*time.Second), // Optional: Custom polling interval (default: 10s)
)
reader.Start()
defer reader.Stop(context.Background()) // Stop during application shutdown
```

### Database Setup

#### 1. Choose Your Database Dialect

The library supports multiple relational databases. By default, PostgreSQL dialect is used. If your database requires a different dialect, configure the appropriate during initialization. Supported dialects are PostgreSQL, MySQL, MariaDB, SQLite, Oracle and SQL Server.

```go
// Example changing dialect to MySQL
outbox.SetSQLDialect(outbox.MySQLDialect)
```

#### 2. Create the Outbox Table

Create an outbox table in your database with the following structure. The table stores messages that need to be published to your message broker:

```sql
-- For Postgres
CREATE TABLE IF NOT EXISTS Outbox (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    context BYTEA NOT NULL,
    payload BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON Outbox (created_at);

-- For Oracle
CREATE TABLE Outbox (
    id RAW(16) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);

-- For MySQL
CREATE TABLE IF NOT EXISTS Outbox (
    id BINARY(16) PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);
```

## Examples

Complete working examples for different databases and message brokers:

- [Postgres & Kafka](./examples/postgres-kafka/service.go) 
- [Oracle & NATS](./examples/oracle-nats/service.go) 
- [MySQL & RabbitMQ](./examples/mysql-rabitmq/service.go) 

To run them:

```bash
cd examples/postgres-kafka # or examples/oracle-nats or examples/mysql-rabitmq
../../scripts/up-and-wait.sh
go run service.go

# In another terminal trigger a POST to trigger entity creation
curl -X POST http://localhost:8080/entity
```