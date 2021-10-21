package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging

import scala.util._
import io.opentargets.etl.backend._
import io.opentargets.etl.backend.drug.Drug
import io.opentargets.etl.backend.graph.EtlDag

object ETL extends LazyLogging {

  def applySingleStep(step: String)(implicit context: ETLSessionContext): Unit = {
    step match {
      case "targetValidation" =>
        logger.info("run step targetValidation")
        TargetValidation()
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

        // build ETL DAG graph
        val etlDag = new EtlDag[String](otContext.configuration.etlDag.steps)

        val etlSteps =
          if (steps.isEmpty) etlDag.getAll
          else if (otContext.configuration.etlDag.resolve) etlDag.getDependenciesFor(steps: _*)
          else steps

        logger.info(s"Steps to execute: $etlSteps")

        etlSteps.foreach { step =>
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
