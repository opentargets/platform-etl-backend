package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging

import scala.util._
import io.opentargets.etl.backend._
import io.opentargets.etl.backend.drug.Drug

object ETL extends LazyLogging {
  val implementedSteps = Map(
    "evidence" -> Evidence,
    "search" -> Search
  )

  def applySingleStep(step: String)(implicit context: ETLSessionContext): Unit = {
    step match {
      case "test" =>
        ETLPipeline
      case "evidence" =>
        logger.info("run step evidence")
        Evidence()
      case "search" =>
        logger.info("run step search")
        Search()
      case "drug" =>
        logger.info("run step drug")
        Drug()
      case "knownDrugs" =>
        logger.info("run step knownDrugs")
        KnownDrugs()
      case "expression" =>
        logger.info("run step expression")
        Expression()
      case "disease" =>
        logger.info("run step disease")
        Disease()
      case "target" =>
        logger.info("run step target")
        target.Target()
      case "reactome" =>
        logger.info("run step reactome (rea)")
        Reactome()
      case "go" =>
        logger.info("run step go")
        Go()
      case "interactions" =>
        logger.info("run step interactions")
        Interactions()
      case "association" =>
        logger.info("run step association")
        Association()
      case "connections" =>
        logger.info("run step connections")
        Connections()
      case "associationOTF" =>
        logger.info("run step associationOTF")
        AssociationOTF()
      case "fda" =>
        logger.info("Running OpenFDA FAERS step")
        OpenFda()
      case "ebisearch" =>
        logger.info("Running EBI Search step")
        EBISearch()
      case "otar" =>
        logger.info("Running Otar projects step")
        OtarProject()
      case _ => logger.warn(s"step $step is unknown so nothing to execute")
    }
    logger.info(s"finished to run step ($step)")
  }

  def apply(steps: Seq[String]): Unit = {

    ETLSessionContext() match {
      case Right(otContext) =>
        implicit val ctxt: ETLSessionContext = otContext

        val etlSteps =
          if (steps.isEmpty) otContext.configuration.common.defaultSteps
          else steps

        val unknownSteps = etlSteps filterNot otContext.configuration.common.defaultSteps.contains
        val knownSteps = etlSteps filter otContext.configuration.common.defaultSteps.contains

        logger.info(s"valid steps to execute: $knownSteps")
        if (unknownSteps.nonEmpty) logger.warn(s"invalid steps to skip: $unknownSteps")

        knownSteps.foreach { step =>
          logger.debug(s"step to run: '$step'")
          ETL.applySingleStep(step)
        }

      case Left(ex) => logger.error(ex.prettyPrint())
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    ETL(args)
  }
}
