import cats.effect.{ExitCode, IO, IOApp}
import service.{DataprocCluster, DataprocWorkflow, ResourceTransfer, WorkflowTemplateService}
import com.google.cloud.dataproc.v1.{WorkflowTemplate, WorkflowTemplateServiceClient}
import model.{Configuration, OpenTargetsWorkflow}
import org.typelevel.log4cats.{LoggerFactory, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.loggerFactoryforSync

// todo: when private workflow selected, move files first

// todo: create necessary configuration for PPP based on simplified config

// todo: call sbt assembly and push jar to specified location
object Main extends IOApp {

  val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger
  def executeWorkflow(resourceLocationName: String,
                      template: WorkflowTemplate,
                      client: WorkflowTemplateServiceClient
  ): IO[Unit] =
    IO(client.instantiateInlineWorkflowTemplateAsync(resourceLocationName, template).get())

  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- logger.info("Starting workflow application.")
      config <- logger.info("Loading configuration") >> Configuration.load
      otWorkflow <- IO(OpenTargetsWorkflow.getWorkflow(args).run(config)).flatTap(otwf =>
        logger.info(otwf.logOpenTargetsWorkflow)
      )
      _ <- ResourceTransfer.execute(otWorkflow.resourcesToMove)
      cluster <- IO(DataprocCluster.createWorkflowTemplatePlacement.run(config.cluster))
      location <- IO(DataprocWorkflow.getGcpLocation.run(config)).flatTap(loc =>
        logger.info(s"Location selected: $loc")
      )
      workflowClient <- IO(WorkflowTemplateService.getWorkflowTemplateServiceClient.run(config))
      workflow <- IO(DataprocWorkflow.createWorkflow(otWorkflow.jobs, cluster))
      _ <- executeWorkflow(location, workflow, workflowClient)
    } yield ExitCode.Success
}
