<p align="center" class="disable-logo">
<a href="#"><img src="assets/logo.png" width="200"/></a>
</p>


[![Release](https://img.shields.io/github/release/oagudo/outbox.svg?style=flat-square)](https://github.com/oagudo/outbox/releases/latest)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE)
![GitHub Actions](https://github.com/oagudo/outbox/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/oagudo/outbox/graph/badge.svg?token=KH1GUAV4VR)](https://codecov.io/gh/oagudo/outbox)
[![Go Report Card](https://goreportcard.com/badge/github.com/oagudo/outbox?style=flat-square)](https://goreportcard.com/report/github.com/oagudo/outbox)
[![Go Reference](https://pkg.go.dev/badge/github.com/oagudo/outbox.svg)](https://pkg.go.dev/github.com/oagudo/outbox)

Lightweight library for the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) in Go, not tied to any specific relational database or broker.

## Key Features

- **Lightweight:** Adds only one external dependency: [google/uuid](https://github.com/google/uuid)
- **Database Agnostic:** Works with PostgreSQL, MySQL, Oracle and other relational databases.
- **Message Broker Agnostic:** Works with any message broker or external system.
- **Easy integration:** Designed for easy integration into your own projects.
- **Observability:** Exposes channels for processing errors and discarded messages that you can connect to your metrics and alerting systems.
- **Fast Publishing:** Optional immediate async message publishing after transaction commit for reduced latency, with guaranteed delivery fallback.
- **Configurable Retry & Backoff Policies:** Fixed, exponential or custom backoff strategies when delivery fails.
- **Max Attempts Safeguard:** Automatically discards poison messages that exceed a configurable `maxAttempts` threshold.

## Usage

The library consists of two main components:

1. **Writer**: Stores your entity and corresponding message atomically within a transaction
2. **Reader**: Publishes stored messages to your message broker in the background

### The Writer

The Writer ensures your entity and outbox message are stored together atomically:

```go
// Initialise Writer
db, _ := sql.Open("pgx", "postgres://...")
dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
writer := outbox.NewWriter(dbCtx)

// --- Option 1: Library-managed transactions (recommended) ---
//
// Write executes user defined queries and store zero, one, or multiple messages atomically.
err = writer.Write(ctx, func(ctx context.Context, tx outbox.TxQueryer, msgWriter outbox.MessageWriter) error {
    // Perform business logic
    result, err := tx.ExecContext(ctx,
        "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2 AND quantity >= $1",
        order.Quantity, order.ProductID)
    if err != nil {
        return err
    }
    rows, _ := result.RowsAffected()
    if rows == 0 {
        return ErrInsufficientInventory // no message emitted, transaction rolled back
    }

    // Create and store outbox message
    payload, _ := json.Marshal(order)
    msg := outbox.NewMessage(payload,
        outbox.WithCreatedAt(order.CreatedAt),
        outbox.WithMetadata(json.RawMessage(`{"trace_id":"abc123"}`)))

    return msgWriter.Store(ctx, msg)
})

// For simple cases that store a single message, use WriteOne
err = writer.WriteOne(ctx, msg, func(ctx context.Context, tx outbox.TxQueryer) error {
    _, err := tx.ExecContext(ctx,
        "INSERT INTO entity (id, created_at) VALUES ($1, $2)",
        entity.ID, entity.CreatedAt)
    return err
})

// --- Option 2: User-managed transactions ---
//
// Intended for users who want to manage the transaction lifecycle themselves
// and only need to persist outbox messages.
unmanagedWriter := writer.Unmanaged()
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

err = unmanagedWriter.Store(ctx, tx, msg)
_, _ = tx.ExecContext(ctx, "INSERT INTO entity (...) VALUES (...)", ...)

tx.Commit()
```

<details>
<summary><strong>🚀 Optimistic Publishing (Optional)</strong></summary>

Optimistic publishing attempts to publish messages immediately after transaction commit, reducing latency while maintaining guaranteed delivery through the background reader as fallback.

#### How It Works

1. Transaction commits (entity + outbox message stored)
2. Immediate publish attempt to broker (asynchronously, will not block the incoming request)
3. On success message is removed from outbox
4. On failure background reader handles delivery later

#### Configuration

```go
// Create publisher (see Reader section below)
publisher := &messagePublisher{}

// Enable optimistic publishing in writer
writer := outbox.NewWriter(dbCtx, outbox.WithOptimisticPublisher(publisher))
```

**Important considerations:**
- Publishing happens asynchronously after transaction commit
- Message consumers must be idempotent as messages could be published twice - by the optimistic publisher and by the reader (Note: consumer idempotency is a good practice regardless of optimistic publishing, though some brokers also provide deduplication features)
- Publishing failures don't affect your transactions - they don't cause `Write()` to fail
- Optimistic publisher is not triggered for user managed transactions - `Writer.Unmanaged()`.

</details>

### The Reader

The Reader periodically checks for unsent messages and publishes them to your message broker:

```go
// Create a message publisher implementation
type messagePublisher struct {
    // Your message broker client (e.g., Kafka, RabbitMQ)
}
func (p *messagePublisher) Publish(ctx context.Context, msg *outbox.Message) error {
    // Publish the message to your broker. See examples below for specific implementations
    return nil
}

// Create and start the reader
reader := outbox.NewReader(
    dbCtx,                              // Database context
    &messagePublisher{},                // Publisher implementation
    outbox.WithInterval(5*time.Second), // Polling interval (default: 10s)
    outbox.WithReadBatchSize(200),      // Read batch size (default: 100)
    outbox.WithDeleteBatchSize(50),     // Delete batch size (default: 20)
    outbox.WithMaxAttempts(300),        // Discard after 300 attempts (default: MaxInt32)
    outbox.WithExponentialDelay(        // Delay between attempts (default: Exponential; can also use Fixed or Custom)
        500*time.Millisecond,           // Initial delay (default: 200ms)
        30*time.Minute),                // Maximum delay (default: 1h)
)
reader.Start()
defer reader.Stop(context.Background()) // Stop during application shutdown

// Monitor standard processing errors (publish / update / delete / read).
go func() {
    for err := range reader.Errors() {
        switch e := err.(type) {
        case *outbox.PublishError:
            log.Printf("Failed to publish message | ID: %s | Error: %v",
                e.Message.ID, e.Err)

        case *outbox.UpdateError:
            log.Printf("Failed to update message | ID: %s | Error: %v",
                e.Message.ID, e.Err)

        case *outbox.DeleteError:
            log.Printf("Batch message deletion failed | Count: %d | Error: %v",
                len(e.Messages), e.Err)
            for _, msg := range e.Messages {
                log.Printf("Failed to delete message | ID: %s", msg.ID)
            }

        case *outbox.ReadError:
            log.Printf("Failed to read outbox messages | Error: %v", e.Err)

        default:
            log.Printf("Unexpected error occurred | Error: %v", e)
        }
    }
}()

// Monitor discarded messages (hit the max-attempts threshold).
go func() {
    for msg := range reader.DiscardedMessages() {
        log.Printf("outbox message %s discarded after %d attempts",
            msg.ID, msg.TimesAttempted)
        // Example next steps:
        //   • forward to a dead-letter topic
        //   • raise an alert / metric
        //   • persist for manual inspection
    }
}()

```

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
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata BYTEA,
    payload BYTEA NOT NULL,
    times_attempted INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox (created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_scheduled_at ON outbox (scheduled_at);
```
</details>

<details>
<summary><strong>📊 MySQL</strong></summary>

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id BINARY(16) PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL,
    scheduled_at TIMESTAMP(3) NOT NULL,
    metadata BLOB,
    payload BLOB NOT NULL,
    times_attempted INT NOT NULL
);

CREATE INDEX idx_outbox_created_at ON outbox (created_at);
CREATE INDEX idx_outbox_scheduled_at ON outbox (scheduled_at);
```
</details>

<details>
<summary><strong>🐬 MariaDB</strong></summary>

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL,
    scheduled_at TIMESTAMP(3) NOT NULL,
    metadata BLOB,
    payload BLOB NOT NULL,
    times_attempted INT NOT NULL
);

CREATE INDEX idx_outbox_created_at ON outbox (created_at);
CREATE INDEX idx_outbox_scheduled_at ON outbox (scheduled_at);
```
</details>

<details>
<summary><strong>🗃️ SQLite</strong></summary>

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY,
    created_at DATETIME NOT NULL,
    scheduled_at DATETIME NOT NULL,
    metadata BLOB,
    payload BLOB NOT NULL,
    times_attempted INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox (created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_scheduled_at ON outbox (scheduled_at);
```
</details>

<details>
<summary><strong>🏛️ Oracle</strong></summary>

```sql
CREATE TABLE outbox (
    id RAW(16) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata BLOB,
    payload BLOB NOT NULL,
    times_attempted NUMBER(10) NOT NULL
);

CREATE INDEX idx_outbox_created_at ON outbox (created_at);
CREATE INDEX idx_outbox_scheduled_at ON outbox (scheduled_at);
```
</details>

<details>
<summary><strong>🪟 SQL Server</strong></summary>

```sql
CREATE TABLE outbox (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    created_at DATETIMEOFFSET(3) NOT NULL,
    scheduled_at DATETIMEOFFSET(3) NOT NULL,
    metadata VARBINARY(MAX),
    payload VARBINARY(MAX) NOT NULL,
    times_attempted INT NOT NULL
);

CREATE INDEX idx_outbox_created_at ON outbox (created_at);
CREATE INDEX idx_outbox_scheduled_at ON outbox (scheduled_at);
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

## FAQ

### What happens when multiple instances of my service use the library?

When running multiple instances of your service, each with its own reader, be aware that:

- Multiple readers will independently retrieve messages. This can result in messages published more than once. To handle this you can either:
  1. Ensure your consumers are idempotent and accept duplicates
  2. Use broker deduplication features if available (e.g. NATS JetStream's Msg-Id)
  3. Run the reader in a single instance only (e.g. single replica deployment in k8s with reader)

The optimistic publisher feature can significantly reduce the number of duplicates. With optimistic publisher messages are delivered as soon as they are committed, so readers will usually see no messages in the outbox table.

Also note that even in single instance deployments, message duplicates can still occur (e.g. if the service crashes right after successfully publishing to the broker). However, these duplicates are less frequent than when you are running multiple reader instances.

### How to instantiate a `DBContext` when using `pgxpool` ?

You can use `stdlib.OpenDBFromPool` [function](https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib#OpenDBFromPool) to get a `*sql.DB` from a `*pgxpool.Pool`.

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/jackc/pgx/v5/stdlib"
    "github.com/oagudo/outbox"
)

// ...
pool, _ := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
db := stdlib.OpenDBFromPool(pool)
dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.