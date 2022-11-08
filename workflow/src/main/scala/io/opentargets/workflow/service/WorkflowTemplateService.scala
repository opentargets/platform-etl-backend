package io.opentargets.workflow.service

import cats.data.Reader
import com.google.cloud.dataproc.v1.{WorkflowTemplateServiceClient, WorkflowTemplateServiceSettings}
import io.opentargets.workflow.model.WorkflowConfiguration

object WorkflowTemplateService {

  /** @return
    *   URL to communicate with the dataproc APIs of the configured region.
    */
  private def getGcpUrl: Reader[WorkflowConfiguration, String] =
    Reader(conf => s"${conf.gcpSettings.region}-dataproc.googleapis.com:443")

  /** @return
    *   settings object with set endpoint to initialise workflow template service client.
    */
  private def getWorkflowTemplateServiceSettings
      : Reader[WorkflowConfiguration, WorkflowTemplateServiceSettings] =
    getGcpUrl.map(url => WorkflowTemplateServiceSettings.newBuilder.setEndpoint(url).build)

  /** @return
    *   configured workflow template service client which can be used to run a workflow template.
    */
  def getWorkflowTemplateServiceClient
      : Reader[WorkflowConfiguration, WorkflowTemplateServiceClient] =
    getWorkflowTemplateServiceSettings.map(WorkflowTemplateServiceClient.create)

}
