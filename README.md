# Outbox

Simple library for [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) in Go

Designed to work with any relational database (such as PostgreSQL, MySQL, CockroachDB, etc.) and any message broker (including Kafka, NATS, RabbitMQ, and more).

## Key Features

- **Database Agnostic:** Designed to work with PostgreSQL, MySQL, CockroachDB, and other relational databases.
- **Message Broker Agnostic:** Integrates seamlessly with popular brokers like Kafka, NATS, RabbitMQ, and others.
- **Simplicity:** Minimal, easy-to-understand codebase focused on core outbox pattern concepts.
- **Extensible:** Designed for easy customization and integration into your own projects.

## Examples

You can find examples for different databases and message brokers:

- [Postgres & Kafka](./examples/postgres-kafka/service.go) 

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your system

### Starting the PostgreSQL Database

To start the PostgreSQL database with the proper schema for the outbox pattern:

```bash
docker-compose up -d
```

## Development

### Linting

The project uses golangci-lint for code quality. Run linting with:

```bash
make lint
```

To install golangci-lint locally:

```bash
# macOS
brew install golangci-lint

# Linux
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin
```