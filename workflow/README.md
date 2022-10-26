# Dataproc workflow utility

This package provides an interface for executing Dataproc workflows. 

There are two workflows available: `public` and `ppp`. 

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
