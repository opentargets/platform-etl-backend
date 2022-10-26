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

/** @param arg
  *   name of step to execute and default job id for linking jobs.
  * @param name
  *   if arg is an invalid job name, this provides a fallback.
  * @param deps
  *   list of dependent step IDs (found in the arg field of other Job instances) which must be run
  *   before this job.
  */
case class Job(arg: String, name: Option[String], deps: Option[Seq[String]]) {
  def getJobId: String = name.getOrElse(arg)
}

case class WorkflowConfiguration(workflowResources: WorkflowResources,
                                 gcpSettings: GcpSettings,
                                 cluster: ClusterSettings,
                                 jobs: Seq[Job]
)

object Configuration {

  def load: IO[WorkflowConfiguration] =
    ConfigSource.default.loadF[IO, WorkflowConfiguration]

}
