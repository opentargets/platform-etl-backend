package io.opentargets.workflow.service

import com.google.cloud.dataproc.v1.{OrderedJob, SparkJob}
import cats.data.Reader
import cats.implicits.catsSyntaxOptionId
import io.opentargets.workflow.model.{Job, Workflow, WorkflowConfiguration, WorkflowResources}

import scala.collection.JavaConverters._

object DataprocJobs {

  private def spaceSeparatedList[A](ls: List[A]): String = ls.mkString("", " ", "")

  /** @return
    *   a Spark job template which needs to be parameterised with a job name. The job name is the
    *   argument passed to the job.
    */
  private def createSparkJobFn: Reader[WorkflowResources, String => SparkJob] = Reader {
    conf => (jobArgument: String) =>
      val javaOptions =
        s"-Dconfig.file=${conf.config.file} ${spaceSeparatedList(conf.javaSettings)}"
      SparkJob.newBuilder
        .setMainJarFileUri(conf.jar.resource)
        .addArgs(jobArgument)
        .addFileUris(conf.config.resource)
        .putProperties("spark.executor.extraJavaOptions", javaOptions)
        .putProperties("spark.driver.extraJavaOptions", javaOptions)
        .build
  }

  private def createJob(job: String): Reader[WorkflowResources, SparkJob] =
    createSparkJobFn.map(fn => fn(job))

  def createOrderedJob(job: Job): Reader[WorkflowResources, OrderedJob] = {
    val orderedJob = for {
      sparkJob <- createJob(job.arg)
    } yield OrderedJob.newBuilder
      .setStepId(job.getJobId)
      .setSparkJob(sparkJob)

    job.deps match {
      case Some(dependency) =>
        orderedJob.map(s => s.addAllPrerequisiteStepIds(dependency.asJava).build)
      case None => orderedJob.map(_.build)
    }
  }

  /** @return
    *   workflow name -> list of steps. If no steps specified in the configuration, all steps are
    *   selected by default.
    */
  def getWorkflowsMap(conf: WorkflowConfiguration): Map[String, List[String]] =
    conf.workflows
      .map(wf => (wf.name, wf.steps))
      .map(i =>
        (i._1,
         i._2 match {
           case Some(jobs) => jobs
           case None       => conf.jobs.map(_.getJobId).toList
         }
        )
      )
      .toMap

  /** @param requestedJobs
    *   list of jobs by name
    * @param allJobs
    *   all available jobs
    * @return
    *   updated list of jobs with configured dependencies. `requestedJobs` which are not in the list
    *   of `allJobs` are removed. Dependencies are updated based on the jobs provided in
    *   `requestedJobs`. Only specified jobs will be added to the workflow, not their upstream
    *   dependencies.
    */
  def filterJobs(requestedJobs: List[String], allJobs: Seq[Job]): Seq[Job] = {
    val jobSet = requestedJobs.toSet
    val filterSelectedJobs = allJobs.filter(j => jobSet.contains(j.getJobId))
    // remove dependencies on selected jobs, which aren't also in the selected jobs.
    val filterJobSet = filterSelectedJobs.map(_.getJobId).toSet
    filterSelectedJobs.map(job =>
      job.copy(deps = job.deps match {
        case Some(dependencies) =>
          val updatedDeps = dependencies.filter(d => filterJobSet.contains(d))
          if (updatedDeps.isEmpty) Option.empty else updatedDeps.some
        case None => None
      })
    )
  }

  def createdOrderedJobs(workflow: Workflow): Reader[WorkflowConfiguration, List[OrderedJob]] =
    Reader { conf =>
      val workflowMap = getWorkflowsMap(conf)
      val selectedWorkflow: Seq[Job] = workflowMap.get(workflow.name) match {
        case Some(wf) => filterJobs(wf, conf.jobs)
        case None     => conf.jobs
      }
      val orderedJobs = for {
        job <- selectedWorkflow
      } yield createOrderedJob(job).run(conf.workflowResources)
      orderedJobs.toList
    }

}
