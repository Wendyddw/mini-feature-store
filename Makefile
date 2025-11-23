.PHONY: help demo setup build test clean docker-up docker-down

help: ## Display this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Initial setup: create directories and setup MinIO buckets
	@echo "Setting up MinIO buckets..."
	@docker-compose -f docker/docker-compose.yml up -d minio
	@sleep 5
	@docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin || true
	@docker exec minio mc mb myminio/warehouse || true
	@echo "Setup complete!"

build: ## Build Spark JAR
	cd spark && sbt assembly

test: ## Run tests
	cd spark && sbt test

demo: setup build ## Run full demo: setup, build, backfill, point-in-time join, sync, serve API
	@echo "=== Starting Feature Store Demo ==="
	@echo ""
	@echo "1. Starting services..."
	@docker-compose -f docker/docker-compose.yml up -d
	@sleep 10
	@echo ""
	@echo "2. Generating sample data..."
	@python scripts/generate_sample_data.py
	@echo ""
	@echo "3. Running backfill pipeline..."
	@cd spark && sbt "runMain com.example.featurestore.App backfill \
		--events-raw-path file:///tmp/events_raw \
		--output-table feature_store.features_daily \
		--start-date 2024-01-01 \
		--end-date 2024-01-07"
	@echo ""
	@echo "4. Running point-in-time join..."
	@cd spark && sbt "runMain com.example.featurestore.App point-in-time-join \
		--labels-path file:///tmp/labels \
		--features-table feature_store.features_daily \
		--output-path file:///tmp/training_data"
	@echo ""
	@echo "5. Syncing to Redis..."
	@cd spark && sbt "runMain com.example.featurestore.App online-sync \
		--features-table feature_store.features_daily \
		--redis-host localhost \
		--redis-port 6379"
	@echo ""
	@echo "6. API is running at http://localhost:8000"
	@echo ""
	@echo "Try these commands:"
	@echo "  curl 'http://localhost:8000/features?user_id=user1'"
	@echo "  curl 'http://localhost:8000/features?user_id=user1&as_of=2024-01-05T12:00:00'"
	@echo ""
	@echo "=== Demo Complete ==="

docker-up: ## Start Docker services
	docker-compose -f docker/docker-compose.yml up -d

docker-down: ## Stop Docker services
	docker-compose -f docker/docker-compose.yml down

clean: ## Clean build artifacts
	cd spark && sbt clean
	rm -rf /tmp/events_raw /tmp/labels /tmp/training_data

