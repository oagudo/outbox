.PHONY: all test start stop lint test-coverage coverage-report clean help

# Default target when just running `make`
all: lint test

start:
	@echo "Starting test dependencies with Docker Compose..."
	cd test && ../scripts/up-and-wait.sh

stop:
	@echo "Stopping Docker resources..."
	cd test && docker compose down

test: start
	@echo "Running tests..."
	go test -v ./...
	@echo "Tests complete."

test-coverage: start
	@echo "Running tests with coverage..."
	go test -v ./... -covermode=atomic -cover -coverpkg=./pkg/outbox -coverprofile=coverage.txt
	@echo "Coverage data written to coverage.txt"

coverage-report:
	@echo "Generating coverage report..."
	go tool cover -html=coverage.txt -o coverage.html
	@echo "Coverage report generated at coverage.html"

coverage-view: coverage-report
	@echo "Opening coverage report in browser..."
	open coverage.html

lint:
	@echo "Running linters..."
	golangci-lint run
	@echo "Linting complete." 

release: ## Release new version
	git tag | grep -q "v${VERSION}" && echo This version was released! Increase VERSION! || git tag "v${VERSION}" && git push origin "v${VERSION}"	