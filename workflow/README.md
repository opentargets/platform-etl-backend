# Dataproc workflow utility

This package provides an interface for executing Open Targets ETL workflows and jobs on Dataproc. 

There are two workflows available: `public` and `ppp`. 

```
ot-etl workflow --help

Usage:
    ot-etl workflow
    ot-etl step

Runs one or more Open Targets ETL jobs on GCP Dataproc.

Options and flags:
    --help
        Display this help text.
    --version, -v
        Print the version number and exit.

Subcommands:
    workflow
        Runs an Open Targets ETL workflow
    step
        Runs an Open Targets ETL step
```

A single job (ETL step) can be executed:

```
Usage: ot-etl step [--config <path>] <step>

Runs an Open Targets ETL step

Options and flags:
    --help
        Display this help text.
    --config <path>, -c <path>
        File overwriting defaults provided in `reference.conf`.
```

For example, to run the `reactome` step with a custom configuration you'd use: `java -jar etl-workflow-assembly-1.0.
0.jar step --config /path/to/config/application.conf reactome`

## Requirements

This utility works on Unix systems and requires the utilities `which` and `gsutil` to be on the `PATH`.

## Running

1. Create fat jar using SBT. From the repository base directory with `sbt workflow/assembly`. This will create a jar 
   file in `<repository>/workflow/target/scala-2.12`
2. Upload to a GCP storage bucket an ETL jar and configuration. These are referenced in the fields 
   `workflow-resources.jar` and `workflow-resources.config`. For preparing these files consult the ETL documentation.
   An example configuration can be as simple as:
```yaml
workflow-resources.config.path = "gs://open-targets-pre-data-releases/test/conf/"
workflow-resources.config.file = "wf_test.conf"
workflow-resources.jar.path = "gs://open-targets-pre-data-releases/development/jars/"
workflow-resources.jar.file = "etl-backend-6be94af.jar"
```

### A workflow 

2. Execute the program [available workflow](#workflows): `java -jar path/to/jar/etl-workflow-assembly-1.0.0.jar 
   workflow [--config <path>] [public|private]`

### A step

2. Execute a single ETL step: `java -jar path/to/jar/etl-workflow-assembly-1.0.0.jar 
   step [--config <path>] reactome`

## <a name="workflows"></a>Available workflows

A workflow is a sequence of ordered jobs which will be executed on a [GCP Dataproc cluster](https://cloud.google.com/dataproc/docs).

There are two workflows available: 
1. Public: Execute all of the steps in the ETL pipeline
2. PPP: Run steps Evidence, Associations, Search, KnownDrugs, EbiSearch, AssociationsOTF

### Partner Preview Platform

The partner preview platform includes additional evidence sources, so the steps which are executed are the 
Evidence step, those which are down-stream of the Evidence step so the new data can cascade. Steps above the 
Evidence step do not need to be re-run. 

Because they do not need to be re-run and some are expensive (notably Literature), the PPP workflow assumes that the 
public workflow has already been run. The workflow attempts to copy _all of the outputs upstream of `evidence`_ to the 
specified PPP output directory. If these files are unavailable the workflow will return an error. 

It is necessary to specify where the existing ETL outputs are located, and where they are to be copied to. These are 
configured using the configuration variables `existing-outputs.path` (public ETL outputs) and `existing-outputs.
copy-to` (path to copy files to).

## Concept overview

At the highest level, a Dataproc Workflow is a specification of:
- the jobs to execute
- the order in which to execute them
- where to execute them

Below is a very high-level overview of the objects used in this utility. Full documentation [is available online](https://cloud.google.com/dataproc/docs/concepts/workflows/overview)

### WorkflowTemplate

A WorkflowTemplate is a specification of a list of OrderedJobs to run and where to run them 
(WorkflowTemplatePlacement). 

### Job

All the jobs currently specified are Spark jobs. A job is a definition of a task to be run and includes information 
about the `jar` to use, files to add on the classpath and configuration options.

### OrderedJob

An OrderedJob adds metadata to a job. It has a unique (valid) step ID and a list of pre-requisite steps which Dataproc 
must run before running this step. 

A valid step ID must: 
- be unique among all jobs within the template.
- contain only letters (a-z, A-Z), numbers (0-9), underscores (_), and hyphens (-). Cannot begin or end with underscore
or hyphen. 
- consist of between 3 and 50 characters.

The step id is used as prefix for job id, as job `goog-dataproc-workflow-step-id` label, and in
[prerequisiteStepIds](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.Dataproc.V1/latest/Google.Cloud.Dataproc.V1.OrderedJob#Google_Cloud_Dataproc_V1_OrderedJob_StepId) field from other
steps.
 
### Managed Cluster

A managed cluster is started and stopped by Dataproc at the beginning/end of the workflow. 

#### Cluster Configuration

Configuration for the number, type and other node settings to be created when the Workflow is finally executed. 

#### WorkflowTemplatePlacement

The WorkflowTemplatePlacement defines a ManagedCluster which will be started and stopped when the WorkflowTemplate 
is executed.

### WorkflowTempateServiceClient

The WorkflowTemplateServiceClient triggers the evaluation of a WorkflowTemplate with the method 
`instantiateInlineWorkflowTemplateAsync(<location>, <WorkflowTemplate>)`

## Authentication

For information on setting up credentials see the [documentation](https://github.com/googleapis/google-cloud-java#authentication)

Run `gcloud auth application-login login` (if you haven't already) to create an authentication environment variable 
so that the workflow can be submitted.

## Working with SBT

This utility is a [submodule](https://www.scala-sbt.org/1.x/docs/Multi-Project.html) within the ETL `sbt` project. 
To run tests, etc preface your sbt commands with `<project / sbtTask`. To run the tests for instance, use `sbt 
workflow / test`