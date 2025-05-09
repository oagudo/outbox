.PHONY: tests start-docker-compose stop-docker-compose

start-docker-compose:
	@echo "Starting test dependencies with Docker Compose..."
	cd test && docker-compose up -d
	@echo "Waiting for PostgreSQL to be healthy..."
	@until docker exec outbox_postgres pg_isready -U postgres; do \
		sleep 1; \
	done
	@echo "PostgreSQL is ready."

stop-docker-compose:
	@echo "Stopping Docker resources..."
	cd test && docker-compose down

tests: start-docker-compose
	@echo "Running tests..."
	go test -v ./test/...
	@echo "Tests complete." 