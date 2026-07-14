.PHONY: up down test lint build clean token emulate swagger

VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")


help:
	@echo "Usage: make [target]"
	@echo "Targets:"
	@echo "  up       - Build and start the Docker containers"
	@echo "  down     - Stop and remove the Docker containers"
	@echo "  test     - Run tests with race detection"
	@echo "  lint     - Run golangci-lint"
	@echo "  build    - Build the Go application"
	@echo "  clean    - Remove the built application"
	@echo "  token    - Generate a token"
	@echo "  emulate  - Emulate producer"

up:
	@COMMIT=$(COMMIT) VERSION=$(VERSION) docker compose up --build -d

down:
	@docker compose down -v

test:
	@go test ./internal/... -race -count=1

lint:
	@golangci-lint run

build: swagger
	@go build -ldflags="-X main.version=$(VERSION) -X main.commit=$(COMMIT)" -o gorder ./cmd

clean:
	@rm -f gorder

token:
	@echo "Generating token..."
	@go run ./cmd/gen-token

swagger:
	@swag init -g cmd/main.go