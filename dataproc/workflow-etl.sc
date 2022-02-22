// import $ivy.`com.google.cloud:google-cloud-storage:2.4.0`
// import $ivy.`com.google.cloud:google-cloud-dataproc:2.3.2`
// https://mvnrepository.com/artifact/com.google.cloud/libraries-bom
//import $ivy.`com.google.cloud:libraries-bom:24.3.0`
// import $ivy.`io.grpc:grpc-protobuf:1.44.0`
// import $ivy.`com.google.protobuf:protobuf-java:3.19.3`

import com.google.cloud.dataproc.v1.{
  ClusterConfig,
  DiskConfig,
  GceClusterConfig,
  InstanceGroupConfig,
  ManagedCluster,
  OrderedJob,
  RegionName,
  SoftwareConfig,
  SparkJob,
  WorkflowTemplate,
  WorkflowTemplatePlacement,
  WorkflowTemplateServiceClient,
  WorkflowTemplateServiceSettings
}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.asJavaIterableConverter

// RELEASE SPECIFIC CONFIGURATION
val bucket = "open-targets-pre-data-releases"
val release = "22.02.3"
val etlJar = "etl-backend-target-4e8fefc.jar"
val literatureJar = "etl-literature-ef90689.jar"

val etlParquetConfig = "22_02_platform_parquet.conf"
val literatureParquetConfig = "2202_literature_parquet.conf"
val etlJsonConf = "22_02_platform_json.conf"
val literatureJsonConf = "2202_literature_json.conf"

// RARELY CHANGED CONFIGURATION
val projectId = "open-targets-eu-dev"
val region = "europe-west1"

val jarPath = s"gs://$bucket/$release/jars"
val configPath = s"gs://$bucket/$release/conf"

val gcpUrl = s"$region-dataproc.googleapis.com:443"

// Configure the jobs within the workflow.
def sparkJob(step: String,
             jar: String,
             config: String,
             jarPath: String = jarPath,
             configPath: String = configPath): SparkJob =
  SparkJob.newBuilder
    .setMainJarFileUri(s"$jarPath/$jar")
    .addArgs(step)
    .addFileUris(s"$configPath/$config")
    .putProperties("spark.executor.extraJavaOptions", s"-Dconfig.file=$config")
    .putProperties("spark.driver.extraJavaOptions", s"-Dconfig.file=$config")
    .build

val disease = "disease"
val reactome = "reactome"
val expression = "expression"
val go = "go-step"
val target = "target"
val interaction = "interaction"
val targetValidation = "targetValidation"
val evidence = "evidence"
val association = "association"
val associationOTF = "associationOTF"
val search = "search"
val drug = "drug"
val knownDrug = "knownDrug"
val ebiSearch = "ebiSearch"
val fda = "fda"
val literature = "literature"

class EtlWorkflowJobs(configEtl: String, configLiterature: String) {

  val diseaseIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(disease)
    .setSparkJob(sparkJob(disease, etlJar, configEtl))
    .build

  val reactomeIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(reactome)
    .setSparkJob(sparkJob(reactome, etlJar, configEtl))
    .build

  val expressionIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(expression)
    .setSparkJob(sparkJob(expression, etlJar, configEtl))
    .build

  val goIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(go)
    .setSparkJob(sparkJob("go", etlJar, configEtl))
    .build

  val targetIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(target)
    .addPrerequisiteStepIds(reactome)
    .setSparkJob(sparkJob(target, etlJar, configEtl))
    .build

  val interactionIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(interaction)
    .addPrerequisiteStepIds(target)
    .setSparkJob(sparkJob(interaction, etlJar, configEtl))
    .build

  val targetValidationIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(targetValidation)
    .addPrerequisiteStepIds(target)
    .setSparkJob(sparkJob(targetValidation, etlJar, configEtl))
    .build

  val evidenceIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(evidence)
    .addAllPrerequisiteStepIds(Iterable(disease, target).asJava)
    .setSparkJob(sparkJob(evidence, etlJar, configEtl))
    .build

  val associationIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(association)
    .addAllPrerequisiteStepIds(Iterable(disease, evidence).asJava)
    .setSparkJob(sparkJob(association, etlJar, configEtl))
    .build

  val associationOtfIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(associationOTF)
    .addAllPrerequisiteStepIds(Iterable(disease, evidence, target, reactome).asJava)
    .setSparkJob(sparkJob(associationOTF, etlJar, configEtl))
    .build

  val searchIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(search)
    .addAllPrerequisiteStepIds(Iterable(target, drug, evidence, association, disease).asJava)
    .setSparkJob(sparkJob(search, etlJar, configEtl))
    .build

  val drugIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(drug)
    .addAllPrerequisiteStepIds(Iterable(target, evidence).asJava)
    .setSparkJob(sparkJob(drug, etlJar, configEtl))
    .build

  val knownDrugIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(knownDrug)
    .addAllPrerequisiteStepIds(Iterable(target, disease, drug, evidence).asJava)
    .setSparkJob(sparkJob(knownDrug, etlJar, configEtl))
    .build

  val ebiSearchIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(ebiSearch)
    .addAllPrerequisiteStepIds(Iterable(target, disease, evidence, association).asJava)
    .setSparkJob(sparkJob(ebiSearch, etlJar, configEtl))
    .build

  val fdaIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(fda)
    .addAllPrerequisiteStepIds(Iterable(drug).asJava)
    .setSparkJob(sparkJob(fda, etlJar, configEtl))
    .build

  val literatureIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(literature)
    .addAllPrerequisiteStepIds(Iterable(disease, target, drug).asJava)
    .setSparkJob(sparkJob("all", literatureJar, configLiterature))
    .build

}

class EtlWorkflow(jobs: EtlWorkflowJobs) {

  // Configure the settings for the workflow template service client.
  lazy val workflowTemplateServiceSettings =
    WorkflowTemplateServiceSettings.newBuilder.setEndpoint(gcpUrl).build

  lazy val workflowTemplateServiceClient: WorkflowTemplateServiceClient =
    WorkflowTemplateServiceClient.create(workflowTemplateServiceSettings)

  lazy val gceClusterConfig = GceClusterConfig.newBuilder
    .setZoneUri(s"$region-d")
    .addTags("etl-cluster")
    .build

  lazy val clusterConfig: ClusterConfig = {
    val softwareConfig = SoftwareConfig.newBuilder
      .setImageVersion("2.0-debian10")
      .build

    val disk = DiskConfig.newBuilder
      .setBootDiskSizeGb(2000)
      .build

    val sparkMasterConfig = {
      InstanceGroupConfig.newBuilder
        .setNumInstances(1)
        .setMachineTypeUri("n1-highmem-64")
        .setDiskConfig(disk)
        .build
    }
    val sparkWorkerConfig = {
      InstanceGroupConfig.newBuilder
        .setNumInstances(4)
        .setMachineTypeUri("n1-highmem-64")
        .setDiskConfig(disk)
        .build
    }
    ClusterConfig.newBuilder
      .setGceClusterConfig(gceClusterConfig)
      .setSoftwareConfig(softwareConfig)
      .setMasterConfig(sparkMasterConfig)
      .setWorkerConfig(sparkWorkerConfig)
      .build
  }

  lazy val managedCluster =
    ManagedCluster.newBuilder.setClusterName("etl-cluster").setConfig(clusterConfig).build
  lazy val workflowTemplatePlacement =
    WorkflowTemplatePlacement.newBuilder.setManagedCluster(managedCluster).build

  // Create the inline workflow template.
  lazy val workflowTemplate = WorkflowTemplate.newBuilder
    .addJobs(jobs.diseaseIndex)
    .addJobs(jobs.reactomeIndex)
    .addJobs(jobs.expressionIndex)
    .addJobs(jobs.goIndex)
    .addJobs(jobs.targetIndex)
    .addJobs(jobs.interactionIndex)
    .addJobs(jobs.targetValidationIndex)
    .addJobs(jobs.evidenceIndex)
    .addJobs(jobs.associationIndex)
    .addJobs(jobs.associationOtfIndex)
    .addJobs(jobs.searchIndex)
    .addJobs(jobs.drugIndex)
    .addJobs(jobs.knownDrugIndex)
    .addJobs(jobs.ebiSearchIndex)
    .addJobs(jobs.fdaIndex)
    .addJobs(jobs.literatureIndex)
    .setPlacement(workflowTemplatePlacement)
    .build

  lazy val parent = RegionName.format(projectId, region)

  def run(): Future[Unit] = Future {

    val instantiateInlineWorkflowTemplateAsync =
      workflowTemplateServiceClient.instantiateInlineWorkflowTemplateAsync(parent, workflowTemplate)

    println("Workflow ran successfully.")
    workflowTemplateServiceClient.close()
  }

}

val jsonJobs = new EtlWorkflowJobs(etlJsonConf, literatureJsonConf)
val parquetJobs = new EtlWorkflowJobs(etlParquetConfig, literatureParquetConfig)

val jsonWorkflow = new EtlWorkflow(jsonJobs)
val parquetWorkflow = new EtlWorkflow(parquetJobs)

jsonWorkflow.run()
parquetWorkflow.run()

val diseaseIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(disease)
  .setSparkJob(sparkJob(disease, etlJar, etlParquetConfig))
  .build

val reactomeIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(reactome)
  .setSparkJob(sparkJob(reactome, etlJar, etlParquetConfig))
  .build

val expressionIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(expression)
  .setSparkJob(sparkJob(expression, etlJar, etlParquetConfig))
  .build

val goIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(go)
  .setSparkJob(sparkJob("go", etlJar, etlParquetConfig))
  .build

val targetIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(target)
  .addPrerequisiteStepIds(reactome)
  .setSparkJob(sparkJob(target, etlJar, etlParquetConfig))
  .build

val interactionIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(interaction)
  .addPrerequisiteStepIds(target)
  .setSparkJob(sparkJob(interaction, etlJar, etlParquetConfig))
  .build

val targetValidationIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(targetValidation)
  .addPrerequisiteStepIds(target)
  .setSparkJob(sparkJob(targetValidation, etlJar, etlParquetConfig))
  .build

val evidenceIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(evidence)
  .addAllPrerequisiteStepIds(Iterable(disease, target).asJava)
  .setSparkJob(sparkJob(evidence, etlJar, etlParquetConfig))
  .build

val associationIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(association)
  .addAllPrerequisiteStepIds(Iterable(disease, evidence).asJava)
  .setSparkJob(sparkJob(association, etlJar, etlParquetConfig))
  .build

val associationOtfIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(associationOTF)
  .addAllPrerequisiteStepIds(Iterable(disease, evidence, target, reactome).asJava)
  .setSparkJob(sparkJob(associationOTF, etlJar, etlParquetConfig))
  .build

val searchIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(search)
  .addAllPrerequisiteStepIds(Iterable(target, drug, evidence, association, disease).asJava)
  .setSparkJob(sparkJob(search, etlJar, etlParquetConfig))
  .build

val drugIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(drug)
  .addAllPrerequisiteStepIds(Iterable(target, evidence).asJava)
  .setSparkJob(sparkJob(drug, etlJar, etlParquetConfig))
  .build

val knownDrugIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(knownDrug)
  .addAllPrerequisiteStepIds(Iterable(target, disease, drug, evidence).asJava)
  .setSparkJob(sparkJob(knownDrug, etlJar, etlParquetConfig))
  .build

val ebiSearchIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(ebiSearch)
  .addAllPrerequisiteStepIds(Iterable(target, disease, evidence, association).asJava)
  .setSparkJob(sparkJob(ebiSearch, etlJar, etlParquetConfig))
  .build

val fdaIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(fda)
  .addAllPrerequisiteStepIds(Iterable(drug).asJava)
  .setSparkJob(sparkJob(fda, etlJar, etlParquetConfig))
  .build

val literatureIndex: OrderedJob = OrderedJob.newBuilder
  .setStepId(literature)
  .addAllPrerequisiteStepIds(Iterable(disease, target, drug).asJava)
  .setSparkJob(sparkJob("all", literatureJar, literatureParquetConfig))
  .build

// Configure the cluster placement for the workflow.// Configure the cluster placement for the workflow.
val gceClusterConfig = GceClusterConfig.newBuilder
  .setZoneUri(s"$region-d")
  .addTags("etl-cluster")
  .build

val clusterConfig: ClusterConfig = {
  val softwareConfig = SoftwareConfig.newBuilder
    .setImageVersion("2.0-debian10")
    .build

  val disk = DiskConfig.newBuilder
    .setBootDiskSizeGb(2000)
    .build

  val sparkMasterConfig = {
    InstanceGroupConfig.newBuilder
      .setNumInstances(1)
      .setMachineTypeUri("n1-highmem-64")
      .setDiskConfig(disk)
      .build
  }
  val sparkWorkerConfig = {
    InstanceGroupConfig.newBuilder
      .setNumInstances(4)
      .setMachineTypeUri("n1-highmem-64")
      .setDiskConfig(disk)
      .build
  }
  ClusterConfig.newBuilder
    .setGceClusterConfig(gceClusterConfig)
    .setSoftwareConfig(softwareConfig)
    .setMasterConfig(sparkMasterConfig)
    .setWorkerConfig(sparkWorkerConfig)
    .build
}

// Configure the settings for the workflow template service client.
val workflowTemplateServiceSettings =
  WorkflowTemplateServiceSettings.newBuilder.setEndpoint(gcpUrl).build

val workflowTemplateServiceClient: WorkflowTemplateServiceClient =
  WorkflowTemplateServiceClient.create(workflowTemplateServiceSettings)

val managedCluster =
  ManagedCluster.newBuilder.setClusterName("etl-cluster").setConfig(clusterConfig).build
val workflowTemplatePlacement =
  WorkflowTemplatePlacement.newBuilder.setManagedCluster(managedCluster).build

// Create the inline workflow template.
val workflowTemplate = WorkflowTemplate.newBuilder
  .addJobs(diseaseIndex)
  .addJobs(reactomeIndex)
  .addJobs(expressionIndex)
  .addJobs(goIndex)
  .addJobs(targetIndex)
  .addJobs(interactionIndex)
  .addJobs(targetValidationIndex)
  .addJobs(evidenceIndex)
  .addJobs(associationIndex)
  .addJobs(associationOtfIndex)
  .addJobs(searchIndex)
  .addJobs(drugIndex)
  .addJobs(knownDrugIndex)
  .addJobs(ebiSearchIndex)
  .addJobs(fdaIndex)
  .addJobs(literatureIndex)
  .setPlacement(workflowTemplatePlacement)
  .build

val parent = RegionName.format(projectId, region)
val instantiateInlineWorkflowTemplateAsync =
  workflowTemplateServiceClient.instantiateInlineWorkflowTemplateAsync(parent, workflowTemplate)
instantiateInlineWorkflowTemplateAsync.get

// Print out a success message.
println("Workflow ran successfully.")
workflowTemplateServiceClient.close()