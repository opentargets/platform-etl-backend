// import $ivy.`com.google.cloud:google-cloud-storage:2.4.0`
// import $ivy.`com.google.cloud:google-cloud-dataproc:2.3.2`
// https://mvnrepository.com/artifact/com.google.cloud/libraries-bom
//import $ivy.`com.google.cloud:libraries-bom:24.3.0`
// import $ivy.`io.grpc:grpc-protobuf:1.44.0`
// import $ivy.`com.google.protobuf:protobuf-java:3.19.3`

 import com.google.cloud.dataproc.v1.{ClusterConfig, DiskConfig, GceClusterConfig, InstanceGroupConfig, ManagedCluster, OrderedJob, RegionName, SoftwareConfig, SparkJob, WorkflowTemplate, WorkflowTemplatePlacement, WorkflowTemplateServiceClient, WorkflowTemplateServiceSettings}

import scala.jdk.CollectionConverters.asJavaIterableConverter

// RELEASE SPECIFIC CONFIGURATION
val bucket = "open-targets-pre-data-releases"
val release = "partners/22.09"
val etlJar = "etl-ppp-2209.jar"

val etlConfiguration = "22_09_ppp.conf"

// RARELY CHANGED CONFIGURATION
val projectId = "open-targets-eu-dev"
val region = "europe-west1"

val jarPath = s"gs://$bucket/$release/jars"
val configPath = s"gs://$bucket/$release/conf"

val gcpUrl = s"$region-dataproc.googleapis.com:443"


/**
  * Define jobs which can be added to workflow
  *
  * @param configEtl        file to supplement `reference.conf` in the ETL
  */
class EtlWorkflowJobs(configEtl: String) {
  val disease = "disease"
  val reactome = "reactome"
  val expression = "expression"
  val go = "go-step"
  val epmc = "epmc"
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
  val otar = "otar"

  val diseaseIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(disease)
    .setSparkJob(sparkJob(disease, etlJar, configEtl))
    .build
  val otarIndex: OrderedJob = OrderedJob.newBuilder
    .setStepId(otar)
    .addPrerequisiteStepIds(disease)
    .setSparkJob(sparkJob(otar, etlJar, configEtl))
    .build
  val epmcEvidence: OrderedJob = OrderedJob.newBuilder
    .setStepId(epmc)
    .setSparkJob(sparkJob(epmc, etlJar, configEtl))
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
    .addAllPrerequisiteStepIds(Iterable(disease, target, epmc).asJava)
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
    .addAllPrerequisiteStepIds(Iterable(target).asJava)
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

}

/**
  * Holder to configure cluster and workflow. Single public method to execute workflow.
  *
  * @param jobs
  */
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
    .addJobs(jobs.otarIndex)
    .addJobs(jobs.epmcEvidence)
    .setPlacement(workflowTemplatePlacement)
    .build

  lazy val parent = RegionName.format(projectId, region)

  def run(): Unit = {
    val wfta = workflowTemplateServiceClient.instantiateInlineWorkflowTemplateAsync(parent, workflowTemplate)
    wfta.get()
    println("Workflow ran successfully.")
    workflowTemplateServiceClient.close()
  }
}

// Create jobs and workflows
//val jsonJobs = new EtlWorkflowJobs(etlJsonConf, literatureJsonConf)
val parquetJobs = new EtlWorkflowJobs(etlConfiguration)
//val jsonWorkflow = new EtlWorkflow(jsonJobs)
val workflow = new EtlWorkflow(parquetJobs)

// wrap in futures to run in parallel
workflow.run()
