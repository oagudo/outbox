# Outbox Pattern

This project provides a simple, modular implementation of the **Transactional Outbox Pattern**. 

It is designed to be easily adaptable to any relational database (such as PostgreSQL, MySQL, CockroachDB, etc.) and any message broker (including Kafka, NATS, RabbitMQ, and more).

The outbox pattern ensures reliable event publishing by storing messages in a database table as part of your application's local transaction. A separate process then reads these messages and publishes them to your chosen message broker, guaranteeing consistency between your database and your event stream.

## Key Features

- **Database Agnostic:** Easily configurable to work with PostgreSQL, MySQL, CockroachDB, and other relational databases.
- **Message Broker Agnostic:** Integrates seamlessly with popular brokers like Kafka, NATS, RabbitMQ, and others.
- **Simplicity:** Minimal, easy-to-understand codebase focused on core outbox pattern concepts.
- **Extensible:** Designed for easy customization and integration into your own projects.

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