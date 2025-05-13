.PHONY: tests start stop lint

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

lint:
	@echo "Running linters..."
	golangci-lint run
	@echo "Linting complete." 

release: ## Release new version
	git tag | grep -q "v${VERSION}" && echo This version was released! Increase VERSION! || git tag "v${VERSION}" && git push origin "v${VERSION}"	