# Outbox

Simple library for [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) in Go

## Key Features

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
// Connect to your database
db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/database")
if err != nil {
    log.Fatalf("failed to connect to database: %v", err)
}

// For MySQL or other database types, set the driver (optional, defaults to PostgreSQL)
outbox.SetDriver(outbox.DriverMySQL)

// Create a writer instance
writer := outbox.NewWriter(db)

// Optional: Enable optimistic publishing to attempt publishing messages
// right after transaction commit (in addition to reader background process)
// writer := outbox.NewWriter(db, outbox.WithOptimisticPublisher(msgPublisher))

// In your business logic:
// 1. Create your entity
entity := Entity{
    ID:        uuid.New(),
    CreatedAt: time.Now().UTC(),
}

// 2. Prepare your message payload and context
entityJSON, _ := json.Marshal(entity)
msgContext := map[string]string{
    "trace_id":       uuid.New().String(),
    "correlation_id": uuid.New().String(),
}
msgContextJSON, _ := json.Marshal(msgContext)

// 3. Create an outbox message
msg := outbox.Message{
    ID:        entity.ID,        // Unique identifier for the message
    CreatedAt: entity.CreatedAt, // When the message was created
    Payload:   entityJSON,       // The actual message content
    Context:   msgContextJSON,   // Additional metadata for the message
}

// 4. Write the message and entity in a single transaction
err = writer.Write(
    ctx, 
    msg, 
    func(ctx context.Context, tx outbox.QueryExecutor) error {
        // This callback executes within a transaction
        // Insert your entity using the provided `tx.ExecContext` function
        return tx.ExecContext(ctx,
            "INSERT INTO Entity (id, created_at) VALUES (?, ?)",
            entity.ID.String(), entity.CreatedAt,
        )
    }
)
```

### The Reader

The Reader periodically checks for unsent messages and publishes them to your message broker:

```go
// 1. Create a message publisher implementation
type messagePublisher struct {
    // Your message broker client (e.g., Kafka, RabbitMQ)
}

// Implement the Publisher interface
func (p *messagePublisher) Publish(ctx context.Context, msg outbox.Message) error {
    // Publish the message to your broker
    // See examples below for specific implementations
    return nil
}

// 2. Create and start the reader
reader := outbox.NewReader(
    db,                           // Same database connection
    &messagePublisher{},          // Your publisher implementation
    outbox.WithInterval(5*time.Second), // Optional: Custom polling interval (default: 10s)
)
reader.Start()

// 3. Stop the reader during application shutdown
reader.Stop()
```

### Database Setup

You will need to create an outbox table in your database with the following structure:

```sql
-- Example for MySQL
CREATE TABLE Outbox (
    id CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);
```

## Examples

Complete working examples for different databases and message brokers:

- [Postgres & Kafka](./examples/postgres-kafka/service.go) 
- [MySQL & RabbitMQ](./examples/mysql-rabitmq/service.go) 

To run them:

```bash
cd examples/mysql-rabitmq # or examples/postgres-kafka
../../scripts/up-and-wait.sh
go run service.go

# in another terminal trigger a POST to trigger entity creation
curl -X POST http://localhost:8080/entity
```