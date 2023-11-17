# This Makefile helper automates bringing up and tearing down a local deployment of Open Targets Platform
 .DEFAULT_GOAL := help

# Targets
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-28s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Build the ETL jar (dataproc)
	@echo "Building JAR for ETL (dataproc)"
	@echo "dataproc: $${ETL_FLAG_DATAPROC}"
	@sbt -J-Xss2M -J-Xmx2G assembly

build_local: ## Build the ETL jar (local)
	@echo "Building JAR for ETL (local)"
	@export ETL_FLAG_DATAPROC=false
	@echo "dataproc: $${ETL_FLAG_DATAPROC}"
	@sbt -J-Xss2M -J-Xmx2G assembly

build_workflow: ## Build the workflow jar
	@echo "Building JAR for workflow"
	@sbt -J-Xss2M -J-Xmx2G workflow/assembly

.PHONY: help build build_local build_workflow