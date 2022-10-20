package model

import cats.effect.IO
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax.CatsEffectConfigSource

case class WorkflowResources(jar: WfResource, config: WfResource)
case class WfResource(path: String, file: String) {
  require(path.endsWith("/"))
  val resource: String = path + file
}
case class GcpSettings(projectId: String, region: String, bucket: String, gcpUrl: String)
case class ClusterSettings(name: String,
                           zone: String,
                           image: String,
                           bootDiskSize: Int,
                           machineType: String,
                           workerCount: Int
)
case class WorkflowConfiguration(wfResource: WorkflowResources,
                                 gcpSettings: GcpSettings,
                                 cluster: ClusterSettings
)

object Configuration {

  def load: IO[WorkflowConfiguration] =
    ConfigSource.default.loadF[IO, WorkflowConfiguration]

}
