<p align="center" class="disable-logo">
<a href="#"><img src="assets/logo.png" width="200"/></a>
</p>


[![Release](https://img.shields.io/github/release/oagudo/outbox.svg?style=flat-square)](https://github.com/oagudo/outbox/releases/latest)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE.md)
![GitHub Actions](https://github.com/oagudo/outbox/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/oagudo/outbox/graph/badge.svg?token=KH1GUAV4VR)](https://codecov.io/gh/oagudo/outbox)
[![Go Report Card](https://goreportcard.com/badge/github.com/oagudo/outbox?style=flat-square)](https://goreportcard.com/report/github.com/oagudo/outbox)
[![Go Reference](https://pkg.go.dev/badge/github.com/oagudo/outbox/v4.svg)](https://pkg.go.dev/github.com/oagudo/outbox)

Lightweight library for the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) in Go, not tied to any specific relational database or broker.

## Key Features

- **Minimal External Dependencies:** Doesn't impose additional dependencies (like specific Kafka, MySQL, etc. libraries) other than [google/uuid](https://github.com/google/uuid) on users of this library.
- **Database Agnostic:** Designed to work with PostgreSQL, MySQL, CockroachDB, and other relational databases.
- **Message Broker Agnostic:** Integrates seamlessly with popular brokers like Kafka, NATS, RabbitMQ, and others.
- **Optimistic Publishing:** Optional immediate async message publishing after transaction commit for reduced latency, with guaranteed delivery fallback.
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

// Create a DBContext and Writer instance
dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
writer := outbox.NewWriter(dbCtx)

// In your business logic:
//
// Create your entity and outbox message
entity := Entity{
    ID:        uuid.New(),
    CreatedAt: time.Now().UTC(),
}

entityJSON, _ := json.Marshal(entity)
msgContext := json.RawMessage(`{"trace_id":"abc123","correlation_id":"xyz789"}`)
msg := outbox.Message{
    ID:        uuid.New(),
    CreatedAt: entity.CreatedAt,
    Payload:   entityJSON,
    Context:   msgContext, // Any relevant metadata for the message
}

// Write message and entity in a single transaction
err = writer.Write(ctx, msg, func(ctx context.Context, txExecFunc outbox.TxExecFunc) error {
    _, err := txExecFunc(ctx, // This query executes within a transaction
        "INSERT INTO Entity (id, created_at) VALUES (?, ?)",
        entity.ID.String(), entity.CreatedAt,
    )
    return err
})
```

<details>
<summary><strong>🚀 Optimistic Publishing (Optional)</strong></summary>

Optimistic publishing attempts to publish messages immediately after transaction commit, reducing latency while maintaining guaranteed delivery through the background reader as fallback.

#### How It Works

1. Transaction commits (entity + outbox message stored)
2. Immediate publish attempt to broker (asynchronously, will not block the incoming request)
3. On success: message is removed from outbox
4. On failure: background reader handles delivery later

#### Configuration

```go
// Create publisher (same as used by reader)
publisher := &messagePublisher{}

// Enable optimistic publishing in writer
writer := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))
```

**Important considerations:**
- Publishing happens asynchronously after transaction commit
- Message consumers must be idempotent as messages could be published twice - by the optimistic publisher and the reader (Note: consumer idempotency is a good practice regardless of optimistic publishing, though some brokers also provide deduplication features)
- Publishing failures don't affect your transactions - they don't cause `Write()` to fail

</details>

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
    dbCtx,                              // DBContext instance
    &messagePublisher{},                // Your publisher implementation
    outbox.WithInterval(5*time.Second), // Optional: Custom polling interval (default: 10s)
)
reader.Start()
defer reader.Stop(context.Background()) // Stop during application shutdown
```

<details>
<summary><strong>🚨 Error Handling</strong></summary>

The Reader provides an error channel to monitor failures during message processing:

```go
reader := outbox.NewReader(dbCtx, &messagePublisher{}, outbox.WithInterval(5*time.Second))
reader.Start()

go func() {
    for err := range reader.Errors() {
        switch err.Op {
        case outbox.OpRead:    // Failed to read from outbox table
            log.Printf("Read error: %v", err.Err)
        case outbox.OpPublish: // Failed to publish message (will retry)
            log.Printf("Publish error for %s: %v", err.Msg.ID, err.Err)
        case outbox.OpDelete:  // Failed to delete published message
            log.Printf("Delete error for %s: %v", err.Msg.ID, err.Err)
        }
    }
}()
```
</details>

### Database Setup

#### 1. Choose Your Database Dialect

The library supports multiple relational databases. Configure the appropriate `SQLDialect` when creating the `DBContext`. Supported dialects are PostgreSQL, MySQL, MariaDB, SQLite, Oracle and SQL Server.

```go
// Example creating a DBContext with MySQL dialect
dbCtx := outbox.NewDBContext(db, outbox.SQLDialectMySQL)
```

#### 2. Create the Outbox Table

The outbox table stores messages that need to be published to your message broker. Choose your database below:

<details>
<summary><strong>🐘 PostgreSQL</strong></summary>

```sql
CREATE TABLE IF NOT EXISTS Outbox (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    context BYTEA NOT NULL,
    payload BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON Outbox (created_at);
```
</details>

<details>
<summary><strong>📊 MySQL</strong></summary>

```sql
CREATE TABLE IF NOT EXISTS Outbox (
    id BINARY(16) PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);
```
</details>

<details>
<summary><strong>🐬 MariaDB</strong></summary>

```sql
CREATE TABLE IF NOT EXISTS Outbox (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);
```
</details>

<details>
<summary><strong>🗃️ SQLite</strong></summary>

```sql
CREATE TABLE IF NOT EXISTS Outbox (
    id TEXT PRIMARY KEY,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON Outbox (created_at);
```
</details>

<details>
<summary><strong>🏛️ Oracle</strong></summary>

```sql
CREATE TABLE Outbox (
    id RAW(16) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);
```
</details>

<details>
<summary><strong>🪟 SQL Server</strong></summary>

```sql
CREATE TABLE Outbox (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    created_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
    context VARBINARY(MAX) NOT NULL,
    payload VARBINARY(MAX) NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);
```
</details>

## Examples

Complete working examples for different databases and message brokers:

- [Postgres & Kafka](./examples/postgres-kafka/service.go)
- [Oracle & NATS](./examples/oracle-nats/service.go)
- [MySQL & RabbitMQ](./examples/mysql-rabbitmq/service.go)

To run an example:

```bash
cd examples/postgres-kafka # or examples/oracle-nats or examples/mysql-rabitmq
../../scripts/up-and-wait.sh
go run service.go

# In another terminal trigger a POST to trigger entity creation
curl -X POST http://localhost:8080/entity
```
