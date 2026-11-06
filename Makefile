.PHONY: test lint run up up-split down seed smoke itest

test:
	go vet ./... && go test ./...

lint:
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed: https://golangci-lint.run/welcome/install/"; exit 1; }
	golangci-lint run

# Runs the server against DATABASE_URL (for example the compose Postgres).
run:
	go run ./cmd/api

up:
	docker compose up --build -d

# One API container (ROLE=api) plus two worker containers (ROLE=worker), sharing the same database.
up-split:
	docker compose -f docker-compose.yml -f docker-compose.split.yml up --build -d

down:
	docker compose down

# Creates demo tenants, account mappings and one API key each; keys are printed once.
seed:
	docker compose run --rm api seed

# Usage: make smoke KEY=pxk_test_... [URL=http://localhost:8080]
smoke:
	scripts/smoke.sh $(or $(URL),http://localhost:8080) $(KEY)
