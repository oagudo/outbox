.PHONY: tests start stop lint

start:
	@echo "Starting test dependencies with Docker Compose..."
	cd test && docker compose up -d
	@echo "Waiting for PostgreSQL to be healthy..."
	@until docker exec outbox_postgres pg_isready -U postgres; do \
		echo "PostgreSQL is not ready yet - sleeping for 1 second..."; \
		sleep 1; \
	done
	@echo "PostgreSQL is ready."

stop:
	@echo "Stopping Docker resources..."
	cd test && docker compose down

test: start
	@echo "Running tests..."
	go test -v ./...
	@echo "Tests complete."

lint:
	@echo "Running linters..."
	golangci-lint run
	@echo "Linting complete." 