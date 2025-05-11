# Outbox Pattern with PostgreSQL

This project demonstrates the implementation of the Transactional Outbox Pattern.

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