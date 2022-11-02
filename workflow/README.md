# Dataproc workflow utility

This package provides an interface for executing Dataproc workflows. 

There are two workflows available: `public` and `ppp`. 

## Available workflows

A workflow is a sequence of ordered jobs which will be executed on a [GCP Dataproc cluster](https://cloud.google.com/dataproc/docs).

There are two workflows available: 
1. Public: Execute all of the steps in the ETL pipeline
2. PPP: Run steps Evidence, Associations, Search, KnownDrugs, EbiSearch, AssociationsOTF

### Partner Preview Platform

The partner preview platform includes additional evidence sources, so the steps which are executed are the 
Evidence step, those which are down-stream of the Evidence step so the new data can cascade. Steps above the 
Evidence step do not need to be re-run. 

Because they do not need to be re-run and some are expensive (notably Literature), the PPP workflow assumes that the 
public workflow has already been run. The workflow attempts to copy all of the outputs upstream of Evidence to the 
specified PPP output directory. If these files are unavailable the workflow will return an error. 

It is necessary to specify where the existing ETL outputs are located, and where they are to be copied to. These are 
configured using the configuration variables `existing-outputs.path` (public ETL outputs) and `existing-outputs.
copy-to` (path to copy files to).

## Concept overview

At the highest level, a Dataproc Workflow is a specification of:
- the jobs to execute
- the order in which to execute them
- where to execute them

### WorkflowTemplate

A WorkflowTemplate is a specification of a list of OrderedJobs to run and where to run them 
(WorkflowTemplatePlacement). 

### Job

All the jobs currently specified a Spark jobs. A job is a definition of a task to be run and includes information 
about the jar to use, files to add on the classpath and configuration options.

### OrderedJob

An OrderedJob has a unique (valid) step ID and a list of pre-requisite steps which Dataproc must run before running 
this step. 

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

## Setting up

For information on setting up credentials see the [documentation](https://github.com/googleapis/google-cloud-java#authentication)

Run `gcloud auth application-login login` (if you haven't already) to create an authentication environment variable 
so that the workflow can be submitted.
