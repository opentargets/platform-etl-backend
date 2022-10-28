package service

import cats.data.Reader
import com.google.cloud.dataproc.v1.{OrderedJob, WorkflowTemplate, WorkflowTemplatePlacement}
import model.WorkflowConfiguration

object DataprocWorkflow {

  /**
    * @return GCP region from configuration.
    */
  def getGcpRegion: Reader[WorkflowConfiguration, String] = Reader(conf => conf.gcpSettings.region)

  /**
    * @param jobs ordered jobs to execute
    * @param cluster on which to execute jobs
    * @return definition of workflow which can be instantiated on a client.
    */
  def createWorkflow(jobs: List[OrderedJob], cluster: WorkflowTemplatePlacement): WorkflowTemplate =
    jobs.foldLeft(WorkflowTemplate.newBuilder)((wt, job) => wt.addJobs(job)).setPlacement(cluster).build

}
