package io.opentargets.workflow.model

import cats.data.Reader
import com.google.cloud.dataproc.v1.OrderedJob
import OpenTargetsWorkflow.ResourcesToMove
import cats.implicits.catsSyntaxEq
import service.DataprocJobs

/** An Open Targets workflow contains the metadata necessary to run the ETL for either the public or
  * private version. In the private version we want to use most of the outputs of the public
  * version. To accommodate this, we have the `resourcesToMove` which is a list of (in, out) pairs
  * which should be copied so that when the ETL runs all the necessary dependencies are in place.
  * @param jobs
  *   list of ordered jobs with prerequisites configured.
  * @param resourcesToMove
  *   pairs of in/out paths which should be copied.
  */
case class OpenTargetsWorkflow(jobs: List[OrderedJob],
                               resourcesToMove: ResourcesToMove = List.empty
) {

  def logOpenTargetsWorkflow: String = {
    def mkString[T](s: List[T]): String = s.mkString("[", ",", "]\n")
    val jobStr = mkString(jobs)
    val resStr = mkString(resourcesToMove.map(r => s"From: ${r._1} -- To: ${r._2}"))
    s"""
       |Open Targets Workflow
       |===
       |Jobs: $jobStr
       |===
       |Resources to move: $resStr
       |===
       |""".stripMargin
  }
}
sealed trait WorkFlowError
case class StepNotFound(msg: String) extends WorkFlowError

object OpenTargetsWorkflow {

  type ResourcesToMove = List[(String, String)]
  val defaultWorkflow = "public"

  /** @param stepName
    *   from conf.jobs.arg specifying ETL step to run.
    * @return
    *   ordered job with no dependencies
    */
  def getOrderedJob(
      stepName: String
  ): Reader[WorkflowConfiguration, Either[StepNotFound, OrderedJob]] = Reader { conf =>
    val steps = conf.jobs.filter(j => j.arg === stepName)
    if (steps.nonEmpty) {
      Right(DataprocJobs.createOrderedJob(steps.head.copy(deps = None)).run(conf.workflowResources))
    } else
      Left(
        StepNotFound(
          s"$stepName not a valid job. Valid jobs are ${conf.jobs.map(_.arg).mkString("[,", ",", "]")}"
        )
      )
  }

  /** @param args
    *   from command line
    * @return
    *   OpenTargetsWorkflow configured with a list of ordered jobs and
    */
  def getWorkflow(args: List[String]): Reader[WorkflowConfiguration, OpenTargetsWorkflow] = Reader {
    conf =>
      val wf: String = getWorkflowArg(args, conf)
      val jobs: List[OrderedJob] = DataprocJobs.createdOrderedJobs(wf).run(conf)
      val files: List[(String, String)] = conf.existingOutputs.toFrom
      OpenTargetsWorkflow(jobs, files)
  }

  def getWorkflowArg(args: List[String], conf: WorkflowConfiguration): String = {
    val workflows = conf.workflows.map(_.name).toSet
    if (args.isEmpty) defaultWorkflow
    else {
      if (workflows.contains(args.head)) args.head else defaultWorkflow
    }
  }

}
