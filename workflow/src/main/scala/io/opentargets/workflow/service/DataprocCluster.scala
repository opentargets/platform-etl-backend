package io.opentargets.workflow.service

import cats.data.Reader
import com.google.cloud.dataproc.v1._
import io.opentargets.workflow.model.ClusterSettings

/** Create a new 'cluster' to run Dataproc jobs.
  *
  * The 'cluster' is a WorkflowTemplatePlacement. This is the specification of a cluster which is
  * parameterised with a series of jobs.
  *
  * The single public method `createWorkflowTemplatePlacement` requires the application
  * configuration and will create a cluster based on the values in `reference.conf`
  */
object DataprocCluster {

  type ClusterR[T] = Reader[ClusterSettings, T]
  private def createSoftwareConfig: ClusterR[SoftwareConfig] = Reader { cs =>
    val sc = SoftwareConfig.newBuilder
      .setImageVersion(cs.image)
    cs.workerCount match {
      case 0 =>
        sc
          .putProperties("dataproc:dataproc.allow.zero.workers", "true")
          .build()
      case _ => sc.build()
    }
  }

  private def createGceClusterConfig: ClusterR[GceClusterConfig] =
    Reader(cs => GceClusterConfig.newBuilder.setZoneUri(cs.zone).build)

  private def createDiskConfig: ClusterR[DiskConfig] =
    Reader(cs =>
      DiskConfig.newBuilder.setBootDiskSizeGb(cs.bootDiskSize).setBootDiskType(cs.diskType).build
    )

  private def createInstanceGroupConfig(disk: DiskConfig): ClusterR[InstanceGroupConfig.Builder] =
    Reader { cs =>
      InstanceGroupConfig.newBuilder
        .setMachineTypeUri(cs.machineType)
        .setDiskConfig(disk)
        .setIsPreemptible(false)
    }
  private def createMasterConfig(disk: DiskConfig): ClusterR[InstanceGroupConfig] =
    createInstanceGroupConfig(disk).map(igc => igc.setNumInstances(1).build)
  private def createWorkerConfig(disk: DiskConfig): ClusterR[InstanceGroupConfig] = Reader { conf =>
    val igc = createInstanceGroupConfig(disk)
    igc.map(igcb => igcb.setNumInstances(conf.workerCount).build).run(conf)
  }

  /** Enabling endpoint-config allows us to access the Spark UI from without Dataproc without having
    * to set up ssh tunnels.
    */
  private def endpointConfig =
    EndpointConfig.newBuilder.setEnableHttpPortAccess(true).build
  private def createClusterConfig: ClusterR[ClusterConfig] =
    for {
      gceConf <- createGceClusterConfig
      diskConf <- createDiskConfig
      masterConf <- createMasterConfig(diskConf)
      workerConf <- createWorkerConfig(diskConf)
      softwareConf <- createSoftwareConfig
    } yield ClusterConfig.newBuilder
      .setGceClusterConfig(gceConf)
      .setSoftwareConfig(softwareConf)
      .setMasterConfig(masterConf)
      .setWorkerConfig(workerConf)
      .setEndpointConfig(endpointConfig)
      .build

  private def getClusterName: ClusterR[String] = Reader(sc => sc.name)

  private def createManagedCluster: ClusterR[ManagedCluster] = for {
    name <- getClusterName
    conf <- createClusterConfig
  } yield ManagedCluster.newBuilder.setClusterName(name).setConfig(conf).build

  /*
  Obtain the cluster by calling `createWorkflowTemplacePlacement.run(conf)` where
  conf is the application configuration.
   */
  def createWorkflowTemplatePlacement: ClusterR[WorkflowTemplatePlacement] =
    createManagedCluster.map(mc => WorkflowTemplatePlacement.newBuilder.setManagedCluster(mc).build)
}
