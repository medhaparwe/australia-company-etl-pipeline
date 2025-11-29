# Australia Company ETL Pipeline - Makefile

.PHONY: help install install-dev test lint format clean run docker-up docker-down

# Default target
help:
	@echo "Australia Company ETL Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make install      - Install dependencies"
	@echo "  make install-dev  - Install dev dependencies"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linters"
	@echo "  make format       - Format code"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make run          - Run pipeline with sample data"
	@echo "  make run-full     - Run full pipeline"
	@echo "  make docker-up    - Start Docker containers"
	@echo "  make docker-down  - Stop Docker containers"

# Install dependencies
install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt
	pip install pytest pytest-cov black flake8 mypy

# Run tests
test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=src --cov-report=html

# Linting
lint:
	flake8 src/ tests/ --max-line-length=100 --ignore=E501,W503
	mypy src/ --ignore-missing-imports

# Format code
format:
	black src/ tests/ --line-length=100

# Clean build artifacts
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf build/ dist/ htmlcov/ .coverage

# Create directories
setup-dirs:
	mkdir -p data/raw/commoncrawl
	mkdir -p data/raw/abr
	mkdir -p data/processed
	mkdir -p data/output
	mkdir -p logs

# Run pipeline
run: setup-dirs
	python src/pipeline.py --sample --skip-load --max-records 100

run-full: setup-dirs
	python src/pipeline.py --sample --max-records 1000

# Docker commands
docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-build:
	docker-compose build

docker-logs:
	docker-compose logs -f

# Database commands
db-create:
	docker-compose exec postgres psql -U postgres -d companydb -f /docker-entrypoint-initdb.d/init.sql

db-connect:
	docker-compose exec postgres psql -U postgres -d companydb

# Development workflow
dev: install-dev setup-dirs
	@echo "Development environment ready!"

# Run notebooks
notebook:
	jupyter notebook notebooks/

