import cats.effect.{ExitCode, IO, IOApp}
import service.{DataprocCluster, DataprocJobs, DataprocWorkflow, WorkflowTemplateService}
import com.google.cloud.dataproc.v1.{WorkflowTemplate, WorkflowTemplateServiceClient}
import model.{Configuration, WorkflowConfiguration}
import org.typelevel.log4cats.{LoggerFactory, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.loggerFactoryforSync
// todo: add logging

object Main extends IOApp {

  val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger
  val defaultWorkflow = "public"
  def executeWorkflow(resourceLocationName: String,
                      template: WorkflowTemplate,
                      client: WorkflowTemplateServiceClient
  ): IO[Unit] =
    IO(client.instantiateInlineWorkflowTemplateAsync(resourceLocationName, template).get())

  def getWorkflowArg(args: List[String], conf: WorkflowConfiguration): String = {
    val workflows = conf.workflows.map(_.name).toSet
    if (args.isEmpty) defaultWorkflow
    else {
      if (workflows.contains(args.head)) args.head else defaultWorkflow
    }
  }
  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- logger.info("Starting workflow application.")
      config <- Configuration.load
      workflowName <- IO(getWorkflowArg(args, config)).flatTap(wf =>
        logger.info(s"Workflow selected: $wf")
      )
      cluster <- IO(DataprocCluster.createWorkflowTemplatePlacement.run(config.cluster))
      jobs <- IO(DataprocJobs.createdOrderedJobs(workflowName).run(config)).flatTap(js =>
        logger.info(s"Jobs selected: ${js.mkString("[", ",", "]")}")
      )
      location <- IO(DataprocWorkflow.getGcpLocation.run(config)).flatTap(loc =>
        logger.info(s"Location selected: $loc")
      )
      workflowClient <- IO(WorkflowTemplateService.getWorkflowTemplateServiceClient.run(config))
      workflow <- IO(DataprocWorkflow.createWorkflow(jobs, cluster))
      _ <- executeWorkflow(location, workflow, workflowClient)
    } yield ExitCode.Success
}
