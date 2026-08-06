.PHONY: dev dev-down migrate-up migrate-down migrate-create test build clean

# ─── Dependencies ────────────────────────────────────────────────
dev:
	docker compose up -d
	@echo "Waiting for Postgres..."
	@sleep 3
	$(MAKE) migrate-up

dev-down:
	docker compose down

# ─── Migrations ──────────────────────────────────────────────────
MIGRATE_DSN = "postgres://idx_mcp:idx_mcp_dev@localhost:5432/idx_mcp?sslmode=disable"

migrate-up:
	migrate -path db/migrations -database $(MIGRATE_DSN) up

migrate-down:
	migrate -path db/migrations -database $(MIGRATE_DSN) down -all

migrate-create:
	@read -p "Migration name: " name; \
	migrate create -ext sql -dir db/migrations -seq $$name

# ─── Build ────────────────────────────────────────────────────────
build:
	go build -o build/mcp-server ./cmd/mcp-server
	go build -o build/enqueue-daily ./cmd/enqueue-daily

build-all: build

# ─── Test ────────────────────────────────────────────────────────
test:
	go test ./... -v -count=1

test-short:
	go test ./... -short -count=1

# ─── Wire ────────────────────────────────────────────────────────
wire:
	cd internal/config && wire

# ─── Clean ────────────────────────────────────────────────────────
clean:
	rm -rf build/
	docker compose down -v
