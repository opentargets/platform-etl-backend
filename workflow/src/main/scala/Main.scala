import cats.effect.{ExitCode, IO, IOApp}
import service.{DataprocCluster, DataprocJobs, DataprocWorkflow, WorkflowTemplateService}
import com.google.cloud.dataproc.v1.{WorkflowTemplate, WorkflowTemplateServiceClient}
import model.Configuration

object Main extends IOApp {

  def executeWorkflow(resourceLocationName: String,
                      template: WorkflowTemplate,
                      client: WorkflowTemplateServiceClient
  ): IO[Unit] =
    IO(client.instantiateInlineWorkflowTemplateAsync(resourceLocationName, template).get())

  val availableWorkflows = Set("public", "private")

  def run(args: List[String]): IO[ExitCode] =
    for {
      workflowName <-
        if (args.isEmpty)
          IO.raiseError(
            new IllegalArgumentException(
              s"No workflow specified. Available workflows are: ${availableWorkflows.mkString("[", ",", "]")}"
            )
          )
        else IO(args.head)
      config <- Configuration.load
      cluster <- IO(DataprocCluster.createWorkflowTemplatePlacement.run(config.cluster))
      jobs <- IO(DataprocJobs.createdOrderedJobs.run(config))
      location <- IO(DataprocWorkflow.getGcpLocation.run(config))
      workflowClient <- IO(WorkflowTemplateService.getWorkflowTemplateServiceClient.run(config))
      workflow <- IO(DataprocWorkflow.createWorkflow(jobs, cluster))
      _ <- executeWorkflow(location, workflow, workflowClient)
    } yield ExitCode.Success
}
