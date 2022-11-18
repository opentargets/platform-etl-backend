package io.opentargets.workflow.model

import cats.effect.IO
import cats.implicits.catsSyntaxSemigroup
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax.CatsEffectConfigSource

import java.nio.file.Path

case class ExistingOutputs(path: String, copyTo: String, sharedOutputs: List[String]) {
  def toFrom: List[(String, String)] = for {
    file <- sharedOutputs
  } yield (path |+| file, copyTo |+| file)
}
case class WorkflowResources(jar: WfResource, config: WfResource, javaSettings: List[String])
case class WfResource(path: String, file: String) {
  require(path.endsWith("/"), "Path must end in \"/\"")
  val resource: String = path + file
}

/** @param name
  *   of workflow
  * @param steps
  *   to execute. Name of step must have a matching entry in `jobs.job.arg`. If no steps are
  *   specified then all jobs will be run.
  * @param copyExisting
  *   use the configured settings in `existing-outputs` to prepare the output directory with
  *   precomputed results. This is used so that expensive steps can be avoided. Workflows which only
  *   run a series of downstream steps will not be required to re-run upstream steps to satisfy
  *   their dependencies.
  */
case class Workflow(name: String, steps: Option[List[String]], copyExisting: Boolean = false)
case class GcpSettings(projectId: String, region: String, bucket: String, gcpUrl: String)
case class ClusterSettings(name: String,
                           zone: String,
                           image: String,
                           bootDiskSize: Int,
                           diskType: String,
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
                                 workflows: List[Workflow],
                                 jobs: Seq[Job],
                                 existingOutputs: ExistingOutputs
) {
  import cats.implicits.catsSyntaxEq
  require(workflows.exists(filterForPublic))
  def getDefaultWorkflow: Workflow = workflows.filter(filterForPublic).head
  private def filterForPublic(wf: Workflow): Boolean = wf.name === "public"
}

object Configuration {

  def load: IO[WorkflowConfiguration] =
    ConfigSource.default.loadF[IO, WorkflowConfiguration]

  def load(path: Option[Path]): IO[WorkflowConfiguration] = path match {
    case Some(conf) =>
      ConfigSource.file(conf).withFallback(ConfigSource.default).loadF[IO, WorkflowConfiguration]
    case None => load
  }

}
