.PHONY: all test start stop lint test-coverage coverage-view release

# Default target when just running `make`
all: lint test

start:
	@echo "Starting test dependencies with Docker Compose..."
	cd test && ../scripts/up-and-wait.sh

stop:
	@echo "Stopping Docker resources..."
	cd test && docker compose down

lint:
	@echo "Running linters..."
	golangci-lint run
	cd test && golangci-lint run -c ../.golangci.yml
	@echo "Linting complete."

test: start
	@echo "Running tests..."
	go test -v -race ./...
	@echo "Running integration tests..."
	cd test && go test -v -race ./...
	@echo "Tests complete."

test-coverage: start
	@echo "Running tests with coverage..."
	go test -v -race ./... -covermode=atomic -cover -coverpkg=./ -coverprofile=unit-coverage.txt
	cd test && go test -v -race ./... -covermode=atomic -cover -coverpkg=./../ -coverprofile=./../integration-coverage.txt
	go install github.com/wadey/gocovmerge@latest
	gocovmerge unit-coverage.txt integration-coverage.txt > coverage.txt
	go tool cover -html=coverage.txt -o=coverage.html
	@echo "Coverage data written to coverage.txt"

coverage-view: test-coverage
	@echo "Opening coverage report in browser..."
	open coverage.html
	@echo "Coverage report generated at coverage.html"

release: ## Release new version
	git tag | grep -q "v${VERSION}" && echo This version was released! Increase VERSION! || git tag "v${VERSION}" && git push origin "v${VERSION}"	
