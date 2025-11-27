.PHONY: help install test lint clean start stop up down test-local teardown seed health

# Ensure poetry is in PATH
export PATH := $(HOME)/.local/bin:$(PATH)

# Default target
.DEFAULT_GOAL := help

# Colors for output
GREEN := \033[0;32m
NC := \033[0m # No Color

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install Python dependencies using Poetry
	poetry install --with dev,test

install-hooks: ## Install pre-commit hooks
	poetry run pre-commit install

test: ## Run test suite with coverage
	poetry run pytest

test-unit: ## Run unit tests only
	poetry run pytest tests/unit/

test-integration: ## Run integration tests only
	poetry run pytest tests/integration/

test-contract: ## Run contract tests only
	poetry run pytest tests/contract/

test-coverage: ## Generate HTML coverage report
	poetry run pytest --cov=src --cov-report=html
	@echo "Coverage report: htmlcov/index.html"

lint: ## Run linters (ruff, black, mypy)
	poetry run ruff check .
	poetry run black --check .
	poetry run mypy src/

lint-fix: ## Run linters with auto-fix
	poetry run ruff check --fix .
	poetry run black .

security: ## Run security scans (bandit, safety)
	poetry run bandit -r src/
	poetry run safety check

format: ## Format code with black
	poetry run black .

clean: ## Clean build artifacts and caches
	rm -rf build/ dist/ *.egg-info/
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/
	rm -rf htmlcov/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete

start: ## Start all services with Docker Compose
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@$(MAKE) health

stop: ## Stop all services
	docker compose down --volumes --remove-orphans

restart: ## Restart all services
	docker compose restart

logs: ## Follow Docker Compose logs
	docker compose logs -f

logs-app: ## Follow application logs only
	docker compose logs -f api


setup-local: ## One-command local environment setup
	bash scripts/setup_local_env.sh

build: ## Build Docker images
	docker compose build

ps: ## Show running containers
	docker compose ps

metrics: ## Open Prometheus metrics
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@echo "Jaeger: http://localhost:16686"

security-scan: ## Run comprehensive security scans
	$(MAKE) security
	@echo "\nScanning for secrets..."
	poetry run detect-secrets scan

type-check: ## Run mypy type checking
	poetry run mypy src --strict --show-error-codes

pre-commit: ## Run pre-commit hooks manually
	poetry run pre-commit run --all-files

all: clean install install-hooks lint test ## Run all checks and tests

# ============================================
# Phase 6: Local Development and Testing
# ============================================

up: ## Start all services with health checks
	@echo "$(GREEN)Starting all services...$(NC)"
	@cd docker/compose && docker compose up -d
	@echo "$(GREEN)Waiting for services to be healthy...$(NC)"
	@./scripts/setup-local.sh || true

down: ## Stop all services and remove volumes
	@echo "$(GREEN)Stopping all services...$(NC)"
	@cd docker/compose && docker compose down -v --remove-orphans

test-local: ## Run full test suite with local Docker environment
	@echo "$(GREEN)Running full test suite with local Docker environment...$(NC)"
	@echo "Checking Docker services..."
	@cd docker/compose && docker compose ps
	@echo "\n$(GREEN)Running unit tests...$(NC)"
	poetry run pytest tests/unit -v --tb=short
	@echo "\n$(GREEN)Running integration tests...$(NC)"
	poetry run pytest tests/integration -v --tb=short
	@echo "\n$(GREEN)Running E2E tests...$(NC)"
	poetry run pytest tests/e2e -v --tb=short
	@echo "\n$(GREEN)All tests completed!$(NC)"

test-quick: ## Run fast tests only (unit tests)
	@echo "$(GREEN)Running quick tests (unit tests only)...$(NC)"
	poetry run pytest tests/unit -v

test-e2e: ## Run E2E tests only
	@echo "$(GREEN)Running E2E tests...$(NC)"
	poetry run pytest tests/e2e -v

teardown: ## Clean shutdown and cleanup
	@echo "$(GREEN)Running teardown...$(NC)"
	@./scripts/teardown.sh

seed: ## Seed MongoDB with test data
	@echo "$(GREEN)Seeding MongoDB with test data...$(NC)"
	@./scripts/seed-mongodb.sh

health: ## Check health of all services
	@echo "$(GREEN)Checking service health...$(NC)"
	@echo "MongoDB:"
	@docker exec cdc-mongodb mongosh --quiet --eval "db.adminCommand({ ping: 1 })" 2>/dev/null && echo "  ✓ Healthy" || echo "  ✗ Unhealthy"
	@echo "Kafka:"
	@docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null && echo "  ✓ Healthy" || echo "  ✗ Unhealthy"
	@echo "MinIO:"
	@curl -sf http://localhost:9000/minio/health/live >/dev/null && echo "  ✓ Healthy" || echo "  ✗ Unhealthy"
	@echo "PostgreSQL:"
	@docker exec cdc-postgres pg_isready -U postgres 2>/dev/null && echo "  ✓ Healthy" || echo "  ✗ Unhealthy"
	@echo "Kafka Connect:"
	@curl -sf http://localhost:8083/connectors >/dev/null && echo "  ✓ Healthy" || echo "  ✗ Unhealthy"
	@echo "Prometheus:"
	@curl -sf http://localhost:9090/-/healthy >/dev/null && echo "  ✓ Healthy" || echo "  ✗ Unhealthy"
	@echo "Grafana:"
	@curl -sf http://localhost:3000/api/health >/dev/null && echo "  ✓ Healthy" || echo "  ✗ Unhealthy"

watch-logs: ## Watch logs from all services
	@cd docker/compose && docker compose logs -f

watch-delta-writer: ## Watch Delta Writer logs
	@cd docker/compose && docker compose logs -f delta-writer

create-pipeline: ## Create a CDC pipeline (requires COLLECTION parameter)
	@./scripts/create-pipeline.sh $(COLLECTION)

deploy-connector: ## Deploy Debezium connector
	@./scripts/deploy-connector.sh
