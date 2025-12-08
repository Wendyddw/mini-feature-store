.PHONY: help demo setup build test clean clean-all docker-up docker-down

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
	@python3 scripts/generate_sample_data.py
	@echo ""
	@echo "3. Running backfill pipeline..."
	@START_DATE=$$(python3 -c "from datetime import datetime, timedelta; print((datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d'))"); \
	END_DATE=$$(python3 -c "from datetime import datetime; print(datetime.now().strftime('%Y-%m-%d'))"); \
	cd spark && sbt "runMain com.example.featurestore.App backfill \
		--events-raw-path file:///tmp/events_raw \
		--output-table feature_store.features_daily \
		--start-date $$START_DATE \
		--end-date $$END_DATE"
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
		--redis-port 6379 \
		--hours-back 168"
	@echo ""
	@echo "6. API is running at http://localhost:8000"
	@echo ""
	@echo "Try online feature serving:"
	@echo "  curl 'http://localhost:8000/features/online/user1'"
	@echo ""
	@echo "Note: Offline features should be accessed via Spark/SQL (see README)"
	@echo "      A development/debugging endpoint exists for convenience."
	@echo ""
	@echo "=== Demo Complete ==="

docker-up: ## Start Docker services
	docker-compose -f docker/docker-compose.yml up -d

docker-down: ## Stop Docker services
	docker-compose -f docker/docker-compose.yml down

clean: ## Clean build artifacts and sample data
	cd spark && sbt clean
	rm -rf /tmp/events_raw /tmp/labels /tmp/training_data
	@echo "Cleaned build artifacts and sample data"

clean-all: clean ## Clean everything including Docker volumes (removes all data)
	@echo "Stopping Docker services..."
	@docker-compose -f docker/docker-compose.yml down -v
	@echo "Cleaned all artifacts, data, and Docker volumes"

