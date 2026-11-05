.PHONY: test lint run up down

test:
	go vet ./... && go test ./...

lint:
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed: https://golangci-lint.run/welcome/install/"; exit 1; }
	golangci-lint run

run:
	go run ./cmd/api

up:
	docker compose up --build -d

down:
	docker compose down
