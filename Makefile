# This Makefile helper automates the running of the ETL pipeline
.EXPORT_ALL_VARIABLES:
# Set variables
ETL_ACTIVE_PROFILE := .env
include ${ETL_ACTIVE_PROFILE}
ppp ?= false
profile ?= default
commit_hash := $(shell git log --oneline | head -n1 | cut -f1 -d' ')
ETL_JAR_FILENAME = etl-backend-${commit_hash}.jar
PATH_LOCAL_ETL_JAR = target/scala-2.12/${ETL_JAR_FILENAME}
PATH_LOCAL_WORKFLOW_JAR = workflow/target/scala-2.12/workflow-${commit_hash}.jar

# Set variables based on whether ppp or not (public)
ifeq ($(ppp),true)
  ETL_CONFIG_TEMPLATE := ppp.conf
  WORKFLOW_CONFIG_TEMPLATE := workflow_ppp.conf
else
  ETL_CONFIG_TEMPLATE := platform.conf
  WORKFLOW_CONFIG_TEMPLATE := workflow.conf
endif

PATH_LOCAL_ETL_CONFIG = configuration/${OTOPS_DATA_RELEASE_VERSION}_${ETL_CONFIG_TEMPLATE}
PATH_LOCAL_WORKFLOW_CONFIG = configuration/${OTOPS_DATA_RELEASE_VERSION}_${WORKFLOW_CONFIG_TEMPLATE}

ifeq ($(ppp),true)
  CONFIG_CP_CMD := gsutil -m cp ${PATH_LOCAL_ETL_CONFIG} ${OTOPS_GCS_ETL_CONFIG_PATH_PPP}${OTOPS_GCS_ETL_CONFIG_FILE_PPP}
  JAR_CP_CMD := gsutil -m cp -n ${PATH_LOCAL_ETL_JAR} ${OTOPS_GCS_ETL_JAR_PATH_PPP}${ETL_JAR_FILENAME_PPP}
  RUN_CMD := java -jar ${PATH_LOCAL_WORKFLOW_JAR} workflow --config ${PATH_LOCAL_WORKFLOW_CONFIG} private
else
  CONFIG_CP_CMD := gsutil -m cp ${PATH_LOCAL_ETL_CONFIG} ${OTOPS_GCS_ETL_CONFIG_PATH}${OTOPS_GCS_ETL_CONFIG_FILE}
  JAR_CP_CMD := gsutil -m cp -n ${PATH_LOCAL_ETL_JAR} ${OTOPS_GCS_ETL_JAR_PATH}${ETL_JAR_FILENAME}
  RUN_CMD := java -jar ${PATH_LOCAL_WORKFLOW_JAR} workflow --config ${PATH_LOCAL_WORKFLOW_CONFIG} public
endif

# Set run command if step is specified.
ifdef step
  RUN_CMD := java -jar ${PATH_LOCAL_WORKFLOW_JAR} step --config ${PATH_LOCAL_WORKFLOW_CONFIG} ${step}
endif



.DEFAULT_GOAL := help

# Targets
.PHONY: help
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-28s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: set_profile
set_profile: ## Set an active configuration profile, e.g. "make set_profile profile='dev'" (see folder 'profiles')
	@echo "[ETL] Setting active profile '${profile}'"
	@ln -sf profiles/config.${profile} ${ETL_ACTIVE_PROFILE}

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

${PATH_LOCAL_WORKFLOW_JAR}: ## Build the workflow jar
	@echo "[ETL] Building JAR for workflow"
	@sbt -J-Xss2M -J-Xmx2G workflow/assembly

.PHONY: build_workflow
build_workflow: ${PATH_LOCAL_WORKFLOW_JAR} ## Build the workflow jar

.PHONY: config_to_gcs
config_to_gcs: etl_config ## Copy the ETL config to GCS
	@echo "[ETL] Copying ETL config to GCS"
	${CONFIG_CP_CMD}

.PHONY: jar_to_gcs
jar_to_gcs: build ## Copy the ETL jar to GCS
	@echo "[ETL] Copying ETL jar to GCS"
	${JAR_CP_CMD}

.PHONY: workflow_config
workflow_config: ## Initialise the workflow config
	@echo "[ETL] Instantiating Workflow config"
	@echo "[ETL] Writing config to ==> ${PATH_LOCAL_WORKFLOW_CONFIG}"
	@bash ./configuration/templates/init_config.sh configuration/templates/${WORKFLOW_CONFIG_TEMPLATE} ${PATH_LOCAL_WORKFLOW_CONFIG}

.PHONY: etl_run
etl_run: workflow_config jar_to_gcs config_to_gcs build_workflow build ## Run the ETL on a GCP Dataproc cluster. Use 'ppp=true' to run ppp workflow, defaults to public workflow. If you want to run a single step use 'step=<step_name>'.
	@echo "[ETL] Attempting to run ETL on GCP dataproc..."
	@echo "[ETL] Using workflow jar: ${PATH_LOCAL_WORKFLOW_JAR}, config: ${PATH_LOCAL_WORKFLOW_CONFIG}"
	${RUN_CMD}
