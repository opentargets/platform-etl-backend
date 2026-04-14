# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Open Targets Platform ETL backend — a Scala/Spark pipeline that transforms raw biological data (targets, diseases, drugs, evidence, etc.) into API-ready entities for the Open Targets Platform. It is typically run on Google Cloud Dataproc.

## Build Commands

```bash
# Build fat JAR for Dataproc (Spark provided by cluster)
make build
# or equivalently:
sbt -J-Xss2M -J-Xmx2G assembly

# Build fat JAR for local execution (includes Spark dependencies)
make build_local
# sbt -J-Xss2M -J-Xmx2G -DETL_FLAG_DATAPROC=false assembly

# Build locally skipping tests
make build_local_skip_tests

# Run all tests
sbt test

# Run a single test class
sbt "testOnly io.opentargets.etl.backend.target.TargetTest"

# Run the ETL locally (requires application.conf with spark-uri = "local[*]")
java -server -Xms1G -Xmx6G -Dconfig.file=./application.conf -jar target/scala-2.12/etl-*.jar [step1 [step2 ...]]
```

## Architecture

### Entry Point & Step Dispatch

`Main.scala` → `ETL.applySingleStep()` dispatches string step names to their implementations. Available steps: `expression`, `openfda`, `go`, `interaction`, `literature`, `otar`, `reactome`, `search`, `search_ebi`, `search_facet`, `target`.

### Session Context

`ETLSessionContext` bundles the typed config (`OTConfig`) and a `SparkSession`. It's passed implicitly to all steps. Spark is configured from `reference.conf` and optionally overridden by an `application.conf`.

### Configuration System

- `src/main/resources/reference.conf` — default config with all input/output paths (pointing to GCS buckets)
- `Configuration.scala` — PureConfig-based typed config. Uses `CamelCase → SnakeCase` field mapping (HOCON keys use underscores, Scala case class fields use camelCase)
- Override config at runtime: `-Dconfig.file=./application.conf`
- `spark_uri = "local[*]"` in application.conf enables local execution
- `spark_settings.write_mode` controls Spark output mode ("overwrite" is the default)

### I/O Pattern

All file I/O is managed through `spark/IoHelpers.scala`:
- `IOResourceConfig(format, path, options, partitionBy)` — describes a dataset
- `IOResourceConfigurations = Map[String, IOResourceConfig]` — named set of inputs/outputs
- `IOResources = Map[String, IOResource]` — named DataFrames after loading
- Steps call `IoHelpers.readFrom(inputs)` and `IoHelpers.writeTo(outputs)` uniformly

### Step Structure

Each step (e.g., `Target.scala`, `Search.scala`) follows the pattern:
1. Receives implicit `ETLSessionContext`
2. Reads its named inputs from config via `IoHelpers.readFrom`
3. Transforms DataFrames
4. Writes named outputs via `IoHelpers.writeTo`

### Key Subdirectories

- `backend/target/` — most complex step, split into many sub-components (Ensembl, Uniprot, Hgnc, GeneOntology, etc.) that are assembled into the final target index
- `backend/literature/` — processes EPMC co-occurrences, runs Word2Vec embeddings
- `backend/openfda/` — processes FDA adverse events with Monte Carlo significance testing
- `backend/spark/` — shared Spark utilities (`Helpers.scala`, `IoHelpers.scala`)
- `backend/graph/` — graph traversal utilities (JGraphT-based)
- `backend/searchFacet/` — generates faceted search indexes for diseases and targets
- `preprocess/` — standalone preprocessors (Uniprot flat-file parser, GO converter) that run outside the main pipeline

### Testing

- Test framework: ScalaTest (`AnyFlatSpecLike` / `Matchers`)
- `SparkSessionSetup` trait provides a local `SparkSession` for unit tests
- `EtlSparkUnitTest` trait combines `SparkSessionSetup` with config loading for integration-style tests
- Most tests extend one of these two traits

### Spark Dependencies

When `ETL_FLAG_DATAPROC=true` (default), Spark dependencies are marked `"provided"` — the assembly JAR relies on the cluster's Spark installation. Use `-DETL_FLAG_DATAPROC=false` to include Spark in the JAR for local runs.

### Code Style

- `scalafmt` is used for formatting; a pre-commit hook is available in `hooks/pre-commit.scalafmt`
- Scala 2.12.20, sbt 1.10.11, Spark 3.2.4
- `-Xlint:unused` is enabled — avoid unused imports/variables
