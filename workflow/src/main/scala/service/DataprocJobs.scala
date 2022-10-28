package service

import com.google.cloud.dataproc.v1.{OrderedJob, SparkJob}
import cats.data.Reader
import model.{Job, WorkflowConfiguration, WorkflowResources}

import scala.collection.JavaConverters._

object DataprocJobs {

  /** @return
    *   a Spark job template which needs to be parameterised with a job name. The job name is the
    *   argument passed to the job.
    */
  private def createSparkJobFn: Reader[WorkflowResources, String => SparkJob] = Reader {
    conf => (jobArgument: String) =>
      SparkJob.newBuilder
        .setMainJarFileUri(conf.jar.resource)
        .addArgs(jobArgument)
        .addFileUris(conf.config.resource)
        .putProperties("spark.executor.extraJavaOptions", s"-Dconfig.file=${conf.config.file}")
        .putProperties("spark.driver.extraJavaOptions", s"-Dconfig.file=${conf.config.file}")
        .build
  }

  private def createJob(job: String): Reader[WorkflowResources, SparkJob] =
    createSparkJobFn.map(fn => fn(job))

  private def createOrderedJob(job: Job): Reader[WorkflowResources, OrderedJob] = {
    val orderedJob = for {
      sparkJob <- createJob(job.getJobId)
    } yield OrderedJob.newBuilder
      .setStepId(job.getJobId)
      .setSparkJob(sparkJob)

    job.deps match {
      case Some(dependency) =>
        orderedJob.map(s => s.addAllPrerequisiteStepIds(dependency.asJava).build)
      case None => orderedJob.map(_.build)
    }
  }

  def createdOrderedJobs: Reader[WorkflowConfiguration, List[OrderedJob]] = Reader { conf =>
    val orderedJobs = for {
      job <- conf.jobs
    } yield createOrderedJob(job).run(conf.workflowResources)
    orderedJobs.toList
  }

}
