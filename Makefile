# This Makefile helper automates the running of the ETL pipeline
.EXPORT_ALL_VARIABLES:
# Set variables
ETL_ACTIVE_PROFILE := .env
include ${ETL_ACTIVE_PROFILE}
ppp ?= false
profile ?= default
commit_hash := $(shell git log --oneline --abbrev=7 | head -n1 | cut -f1 -d' ')
ETL_JAR_FILENAME = etl-backend-${commit_hash}.jar
PATH_LOCAL_ETL_JAR = target/scala-2.12/${ETL_JAR_FILENAME}


# Set variables based on whether ppp or not (public)
ifeq ($(ppp),true)
  ETL_CONFIG_TEMPLATE := ppp.conf
else
  ETL_CONFIG_TEMPLATE := platform.conf
endif

PATH_LOCAL_ETL_CONFIG = configuration/${OTOPS_DATA_RELEASE_VERSION}_${ETL_CONFIG_TEMPLATE}

.DEFAULT_GOAL := help

# Targets
.PHONY: help
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-28s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: set_profile
set_profile: ## Set an active configuration profile, e.g. "make set_profile profile='dev'" (see folder 'profiles')
	@echo "[ETL] Setting active profile '${profile}'"
	@ln -sf profiles/config.${profile} ${ETL_ACTIVE_PROFILE}

${ETL_ACTIVE_PROFILE}: ## Set the default configuration profile if there is no profile set.
	@ln -sf profiles/config.default ${ETL_ACTIVE_PROFILE}

.PHONY: etl_config
etl_config: ## Instantiate the ETL config.
	@echo "[ETL] Instantiating ETL config"
	@echo "[ETL] Writing config to ==> ${PATH_LOCAL_ETL_CONFIG}"
	@bash ./configuration/templates/init_config.sh configuration/templates/${ETL_CONFIG_TEMPLATE} ${PATH_LOCAL_ETL_CONFIG}

${PATH_LOCAL_ETL_JAR}: ## Target for building the jar.
	@echo "[ETL] Building JAR for ETL (dataproc)"
	@sbt -J-Xss2M -J-Xmx2G assembly

.PHONY: build
build: ${PATH_LOCAL_ETL_JAR} ## Build the ETL jar (dataproc)

.PHONY: build_local
build_local: ## Build the ETL jar for running locally
	@echo "[ETL] Building JAR for running the ETL locally"
	@sbt -J-Xss2M -J-Xmx2G -DETL_FLAG_DATAPROC=false assembly

.PHONY: build_local_skip_tests
build_local_skip_tests: ## Build the ETL jar for running locally, skipping tests
	@echo "[ETL] Building JAR for running the ETL locally with logs"
	@sbt -J-Xss2M -J-Xmx2G -DETL_FLAG_DATAPROC=false 'set test in assembly := {}' assembly

.PHONY: config_to_gcs
config_to_gcs: etl_config ## Copy the ETL config to GCS
	@echo "[ETL] Copying ETL config to GCS"
	${CONFIG_CP_CMD}

.PHONY: jar_to_gcs
jar_to_gcs: build ## Copy the ETL jar to GCS
	@echo "[ETL] Copying ETL jar to GCS"
	${JAR_CP_CMD}
