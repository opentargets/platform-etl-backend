package io.opentargets.workflow.cli

import cats.effect._
import cats.implicits._
import io.opentargets.workflow.cli.CommandLine.{stepOptsCmd, workflowOptsCmd}
import com.monovore.decline._
import com.monovore.decline.effect._
import io.opentargets.workflow.WorkflowOrchestration

import java.nio.file.Path

sealed trait Commands
case class Workflow(name: String, config: Option[Path]) extends Commands
case class Step(name: String, config: Option[Path]) extends Commands

object CommandLine {
  val configFileOpts: Opts[Option[Path]] =
    Opts
      .option[Path]("config",
                    "File overwriting defaults provided in `reference.conf`.",
                    short = "c"
      )
      .orNone

  val workFlowOpts: Opts[String] = Opts.argument[String](metavar = "workflow").withDefault("public")
  val stepOpts: Opts[String] = Opts.argument[String](metavar = "step")

  val workflowOptsCmd: Opts[Workflow] =
    Opts.subcommand("workflow", "Runs an Open Targets ETL workflow") {
      (workFlowOpts, configFileOpts).mapN(Workflow)
    }

  val stepOptsCmd: Opts[Step] = Opts.subcommand("step", "Runs an Open Targets ETL step") {
    (stepOpts, configFileOpts).mapN(Step)
  }

}

object OpenTargetsCliApp
    extends CommandIOApp(
      name = "ot-etl",
      header = "Runs one or more Open Targets ETL jobs on GCP Dataproc.",
      version = "0.0.1"
    ) {

  override def main: Opts[IO[ExitCode]] =
    (workflowOptsCmd orElse stepOptsCmd).map {
      case Workflow(name, config) => WorkflowOrchestration.runWorkflow(name, config)
      case Step(name, config)     => WorkflowOrchestration.runSingleStep(name, config)
    }
}
