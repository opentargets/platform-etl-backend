package io.opentargets.workflow

import cats.effect.IO.fromEither
import cats.effect.{ExitCode, IO}
import com.google.cloud.dataproc.v1.{WorkflowTemplate, WorkflowTemplateServiceClient}
import io.opentargets.workflow.model.{Configuration, OpenTargetsWorkflow}
import org.typelevel.log4cats.slf4j.loggerFactoryforSync
import org.typelevel.log4cats.{LoggerFactory, SelfAwareStructuredLogger}
import service.{DataprocCluster, DataprocWorkflow, ResourceTransfer, WorkflowTemplateService}

import java.nio.file.Path

// todo: when private workflow selected, move files first

// todo: create necessary configuration for PPP based on simplified config

// todo: call sbt assembly and push jar to specified location
object WorkflowOrchestration {

  val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger
  def executeWorkflow(resourceLocationName: String,
                      template: WorkflowTemplate,
                      client: WorkflowTemplateServiceClient
  ): IO[Unit] =
    IO(client.instantiateInlineWorkflowTemplateAsync(resourceLocationName, template).get())

  def runSingleStep(stepName: String, config: Option[Path]): IO[ExitCode] = for {
    _ <- logger.info(s"Running step $stepName.")
    config <- logger.info("Loading configuration") >> Configuration.load(config)
    stepMaybe <- IO(OpenTargetsWorkflow.getOrderedJob(stepName).run(config)).map(s => fromEither(s))
    step <- stepMaybe
    cluster <- IO(DataprocCluster.createWorkflowTemplatePlacement.run(config.cluster))
    location <- IO(DataprocWorkflow.getGcpLocation.run(config)).flatTap(loc =>
      logger.info(s"Location selected: $loc")
    )
    workflowClient <- IO(WorkflowTemplateService.getWorkflowTemplateServiceClient.run(config))
    workflow <- IO(DataprocWorkflow.createWorkflow(_, cluster))
    _ <- executeWorkflow(location, workflow(List(step)), workflowClient)
  } yield ExitCode.Success

  def runWorkflow(workflowName: String, config: Option[Path]): IO[ExitCode] = for {
    _ <- logger.info("Starting workflow application.")
    config <- logger.info("Loading configuration") >> Configuration.load(config)
    otWorkflow <- IO(OpenTargetsWorkflow.getWorkflow(workflowName).run(config)).flatTap(otwf =>
      logger.info(otwf.logOpenTargetsWorkflow)
    )
    _ <- ResourceTransfer.execute(otWorkflow.resourcesToMove)
    cluster <- IO(DataprocCluster.createWorkflowTemplatePlacement.run(config.cluster))
    location <- IO(DataprocWorkflow.getGcpLocation.run(config)).flatTap(loc =>
      logger.info(s"Location selected: $loc")
    )
    workflowClient <- IO(WorkflowTemplateService.getWorkflowTemplateServiceClient.run(config))
    workflow <- IO(DataprocWorkflow.createWorkflow(_, cluster))
    _ <- executeWorkflow(location, workflow(otWorkflow.jobs), workflowClient)
  } yield ExitCode.Success

}
