# Makefile for PgREST development lifecycle
# Provides convenient commands for building, testing, benchmarking, and running

.PHONY: help build build-release clean run run-release test test-unit test-integration test-all test-ignored bench bench-micro bench-integration bench-load fmt lint clippy check doc

# Default target
.DEFAULT_GOAL := help

# Variables
CARGO := cargo
BINARY_NAME := pgrest
BENCH_DIR := benches
TEST_DIR := tests

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

##@ General

help: ## Display this help message
	@echo "$(BLUE)PgREST Development Commands$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make $(BLUE)<target>$(NC)\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  $(BLUE)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(GREEN)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Building

build: ## Build the project in debug mode
	@echo "$(BLUE)Building PgREST...$(NC)"
	$(CARGO) build

build-release: ## Build the project in release mode
	@echo "$(BLUE)Building PgREST (release)...$(NC)"
	$(CARGO) build --release

clean: ## Clean build artifacts
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	$(CARGO) clean

##@ Running

run: build ## Run the server in debug mode
	@echo "$(BLUE)Running PgREST (debug)...$(NC)"
	$(CARGO) run

run-release: build-release ## Run the server in release mode
	@echo "$(BLUE)Running PgREST (release)...$(NC)"
	$(CARGO) run --release

##@ Testing

test: ## Run unit tests only (no Docker required)
	@echo "$(BLUE)Running unit tests...$(NC)"
	$(CARGO) test --lib --tests -- --skip ignored

test-unit: test ## Alias for test

test-integration: ## Run integration tests (requires Docker)
	@echo "$(BLUE)Running integration tests (requires Docker)...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Docker is not running. Integration tests require Docker.$(NC)"; \
		exit 1; \
	fi
	$(CARGO) test --test '*' -- --ignored --test-threads=8

test-all: ## Run all tests including Docker-dependent ones
	@echo "$(BLUE)Running all tests (including Docker-dependent)...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Docker is not running. Some tests require Docker.$(NC)"; \
	fi
	$(CARGO) test --all-features -- --include-ignored --test-threads=8

test-ignored: ## Run only ignored tests (Docker-dependent)
	@echo "$(BLUE)Running ignored tests (Docker-dependent)...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Docker is not running. Ignored tests require Docker.$(NC)"; \
		exit 1; \
	fi
	$(CARGO) test --all-features -- --ignored --test-threads=8

test-parallel: ## Run tests in parallel (faster, but may have Docker conflicts)
	@echo "$(BLUE)Running tests in parallel...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Docker is not running. Some tests require Docker.$(NC)"; \
	fi
	$(CARGO) test --all-features -- --include-ignored

test-e2e: ## Run end-to-end tests in parallel with 8 threads (requires Docker)
	@echo "$(BLUE)Running E2E tests in parallel with 8 threads (requires Docker)...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Error: Docker is not running. E2E tests require Docker.$(NC)"; \
		exit 1; \
	fi
	$(CARGO) test --test e2e_app -- --test-threads=8

test-e2e-serial: ## Run end-to-end tests serially (single-threaded, more stable)
	@echo "$(BLUE)Running E2E tests serially (requires Docker)...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Error: Docker is not running. E2E tests require Docker.$(NC)"; \
		exit 1; \
	fi
	$(CARGO) test --test e2e_app -- --test-threads=1

test-e2e-computed: ## Run computed field E2E tests in parallel with 8 threads (requires Docker)
	@echo "$(BLUE)Running computed field E2E tests in parallel (requires Docker)...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Error: Docker is not running. E2E tests require Docker.$(NC)"; \
		exit 1; \
	fi
	$(CARGO) test --test e2e_app e2e_computed_field -- --test-threads=8

test-e2e-computed-serial: ## Run computed field E2E tests serially (single-threaded)
	@echo "$(BLUE)Running computed field E2E tests serially (requires Docker)...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Error: Docker is not running. E2E tests require Docker.$(NC)"; \
		exit 1; \
	fi
	$(CARGO) test --test e2e_app e2e_computed_field -- --test-threads=1

##@ Benchmarking

bench: ## Run all benchmarks
	@echo "$(BLUE)Running all benchmarks...$(NC)"
	$(CARGO) bench --all

bench-micro: ## Run micro-benchmarks only
	@echo "$(BLUE)Running micro-benchmarks...$(NC)"
	$(CARGO) bench --bench micro_benchmarks

bench-integration: ## Run integration benchmarks (requires running server)
	@echo "$(BLUE)Running integration benchmarks (requires server on localhost:3000)...$(NC)"
	@if ! curl -s http://localhost:3000/ > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Server not running on localhost:3000$(NC)"; \
		echo "$(YELLOW)Start server with: make run-release$(NC)"; \
	fi
	$(CARGO) bench --bench integration_benchmarks

bench-load: ## Run load tests (requires running server)
	@echo "$(BLUE)Running load tests (requires server on localhost:3000)...$(NC)"
	@if ! curl -s http://localhost:3000/ > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Server not running on localhost:3000$(NC)"; \
		echo "$(YELLOW)Start server with: make run-release$(NC)"; \
	fi
	$(CARGO) bench --bench load_tester

bench-setup: ## Setup benchmark database (load schema and seed data)
	@echo "$(BLUE)Setting up benchmark database...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Docker is not running.$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Loading schema...$(NC)"
	@psql -h localhost -U postgres -f $(TEST_DIR)/fixtures/schema.sql || \
		echo "$(YELLOW)Note: If psql fails, ensure PostgreSQL is running and accessible$(NC)"
	@echo "$(BLUE)Loading seed data...$(NC)"
	@psql -h localhost -U postgres -f $(BENCH_DIR)/fixtures/seed_data.sql || \
		echo "$(YELLOW)Note: If psql fails, ensure PostgreSQL is running and accessible$(NC)"

bench-docker-build: ## Build PgREST Docker image for benchmarks
	@echo "$(BLUE)Building PgREST Docker image...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Error: Docker is not running.$(NC)"; \
		exit 1; \
	fi
	docker compose -f docker-compose.bench.yml --env-file .env.bench build

bench-docker-up: bench-docker-build ## Start Docker Compose services for benchmarks
	@echo "$(BLUE)Starting Docker Compose services...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Error: Docker is not running.$(NC)"; \
		exit 1; \
	fi
	docker compose -f docker-compose.bench.yml --env-file .env.bench up -d
	@echo "$(BLUE)Waiting for services to be healthy...$(NC)"
	@timeout=60; \
	while [ $$timeout -gt 0 ]; do \
		if curl -s http://localhost:3000/ > /dev/null 2>&1; then \
			echo "$(GREEN)Services are ready!$(NC)"; \
			break; \
		fi; \
		sleep 2; \
		timeout=$$((timeout - 2)); \
	done; \
	if [ $$timeout -le 0 ]; then \
		echo "$(YELLOW)Warning: Services may not be fully ready$(NC)"; \
	fi

bench-docker-down: ## Stop and remove Docker Compose services
	@echo "$(BLUE)Stopping Docker Compose services...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Docker is not running.$(NC)"; \
		exit 0; \
	fi
	docker compose -f docker-compose.bench.yml --env-file .env.bench down

bench-docker-logs: ## View logs from benchmark containers
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Error: Docker is not running.$(NC)"; \
		exit 1; \
	fi
	docker compose -f docker-compose.bench.yml --env-file .env.bench logs -f

bench-docker: bench-docker-up ## Full Docker benchmark workflow: build, start, run benchmarks, cleanup
	@echo "$(BLUE)Running benchmarks against Docker services...$(NC)"
	@set -e; \
	$(CARGO) bench --bench integration_benchmarks --bench load_tester; \
	bench_exit=$$?; \
	make bench-docker-down; \
	exit $$bench_exit

##@ Code Quality

fmt: ## Format code with rustfmt
	@echo "$(BLUE)Formatting code...$(NC)"
	$(CARGO) fmt

fmt-check: ## Check code formatting without modifying files
	@echo "$(BLUE)Checking code formatting...$(NC)"
	$(CARGO) fmt -- --check

lint: clippy ## Alias for clippy

clippy: ## Run clippy linter
	@echo "$(BLUE)Running clippy...$(NC)"
	$(CARGO) clippy --all-targets --all-features -- -D warnings

check: ## Check code without building
	@echo "$(BLUE)Checking code...$(NC)"
	$(CARGO) check --all-targets

check-all: check clippy fmt-check ## Run all checks (check + clippy + fmt-check)

##@ Documentation

doc: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	$(CARGO) doc --no-deps --open

doc-build: ## Build documentation without opening
	@echo "$(BLUE)Building documentation...$(NC)"
	$(CARGO) doc --no-deps

##@ Development Workflow

dev-setup: ## Setup development environment (check Docker, install deps)
	@echo "$(BLUE)Setting up development environment...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)Warning: Docker is not running. Some tests require Docker.$(NC)"; \
	else \
		echo "$(GREEN)Docker is running$(NC)"; \
	fi
	@echo "$(BLUE)Checking Rust toolchain...$(NC)"
	@rustc --version || (echo "$(YELLOW)Error: Rust is not installed$(NC)" && exit 1)
	@echo "$(GREEN)Development environment ready$(NC)"

ci: fmt-check clippy test-all ## Run CI checks (format, lint, test)

pre-commit: fmt clippy test ## Run pre-commit checks (format, lint, test unit)

##@ Quick Commands

quick-test: build test ## Quick test cycle (build + test)

quick-bench: build-release bench-micro ## Quick benchmark cycle (build release + micro-bench)

all: clean build-release test-all bench-micro ## Run everything (clean, build, test, benchmark)
