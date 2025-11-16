.PHONY: help compile test lint test-lint build clean format format-check

help: ## Display this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

compile: ## Compile the project
	sbt compile

test: ## Run tests
	sbt test

lint: ## Format and fix linting issues
	sbt scalafmtAll scalafixAll

test-lint: ## Check formatting and linting without fixing
	sbt scalafmtCheckAll scalafixCheckAll

format: ## Format code only
	sbt scalafmtAll

format-check: ## Check code formatting only
	sbt scalafmtCheckAll

build: ## Build assembly JAR
	sbt assembly

clean: ## Clean build artifacts
	sbt clean

