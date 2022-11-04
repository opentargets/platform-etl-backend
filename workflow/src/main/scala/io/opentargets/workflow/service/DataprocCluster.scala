package service

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
    SoftwareConfig.newBuilder
      .setImageVersion(cs.image)
      .build
  }

  private def createGceClusterConfig: ClusterR[GceClusterConfig] =
    Reader(cs => GceClusterConfig.newBuilder.setZoneUri(cs.zone).build)

  private def createDiskConfig: ClusterR[DiskConfig] =
    Reader(cs => DiskConfig.newBuilder.setBootDiskSizeGb(cs.bootDiskSize).build)

  private def createInstanceGroupConfig(disk: DiskConfig): ClusterR[InstanceGroupConfig] = Reader {
    cs =>
      InstanceGroupConfig.newBuilder
        .setNumInstances(1)
        .setMachineTypeUri(cs.machineType)
        .setDiskConfig(disk)
        .build
  }

  private def createClusterConfig: ClusterR[ClusterConfig] =
    for {
      gceConf <- createGceClusterConfig
      diskConf <- createDiskConfig
      igConf <- createInstanceGroupConfig(diskConf)
      softwareConf <- createSoftwareConfig
    } yield ClusterConfig.newBuilder
      .setGceClusterConfig(gceConf)
      .setMasterConfig(igConf)
      .setSoftwareConfig(softwareConf)
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
