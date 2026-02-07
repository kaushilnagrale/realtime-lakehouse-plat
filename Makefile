.PHONY: help install infra-up infra-down stream api test lint clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install Python dependencies
	pip install -r requirements.txt

infra-up:  ## Start Kafka, Schema Registry, Prometheus, Grafana
	docker-compose up -d
	@echo "✅ Infrastructure running"
	@echo "  Kafka:           localhost:9092"
	@echo "  Schema Registry: localhost:8081"
	@echo "  Kafka UI:        localhost:8080"
	@echo "  Prometheus:      localhost:9090"
	@echo "  Grafana:         localhost:3000"

infra-down:  ## Stop all infrastructure
	docker-compose down -v

stream:  ## Start the streaming pipeline
	python -m src.processing.stream_processor

api:  ## Start the analytics API
	uvicorn src.serving.api:app --reload --host 0.0.0.0 --port 8000

generate-data:  ## Generate sample data (JSON)
	python -m scripts.produce_sample_data --events 10000 --mode json

produce-kafka:  ## Produce sample events to Kafka
	python -m scripts.produce_sample_data --events 10000 --mode kafka

test:  ## Run all tests
	pytest tests/ -v --tb=short

test-unit:  ## Run unit tests only
	pytest tests/ -v -m "not integration" --tb=short

test-cov:  ## Run tests with coverage
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

lint:  ## Run linting checks
	python -m py_compile src/config/settings.py
	python -m py_compile src/ingestion/kafka_consumer.py
	python -m py_compile src/processing/bronze_layer.py
	python -m py_compile src/processing/silver_layer.py
	python -m py_compile src/processing/gold_layer.py
	python -m py_compile src/serving/api.py
	@echo "✅ All files compile successfully"

clean:  ## Remove generated files and caches
	rm -rf __pycache__ .pytest_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf data/lakehouse data/checkpoints
