# Targets
.PHONY: help
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-28s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build the ETL jar (dataproc)
	@echo "[ETL] Building JAR for ETL (dataproc)"
	@sbt -J-Xss2M -J-Xmx2G assembly
.PHONY: build_local
build_local: ## Build the ETL jar for running locally
	@echo "[ETL] Building JAR for running the ETL locally"
	@sbt -J-Xss2M -J-Xmx2G -DETL_FLAG_DATAPROC=false assembly

.PHONY: build_local_skip_tests
build_local_skip_tests: ## Build the ETL jar for running locally, skipping tests
	@echo "[ETL] Building JAR for running the ETL locally with logs"
	@sbt -J-Xss2M -J-Xmx2G -DETL_FLAG_DATAPROC=false 'set test in assembly := {}' assembly
