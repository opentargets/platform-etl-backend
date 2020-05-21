package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging

import scala.util._
import io.opentargets.etl.backend._

object ETL extends LazyLogging {

  def applySingleStep(step: String)(implicit context: ETLSessionContext) = {
    step match {
      case "search" =>
        logger.info("run step search")
        Search()
      case "associations" =>
        logger.info("run step associations")
        Associations()
      case "associationsLLR" =>
        logger.info("run step associations-llr")
        AssociationsLLR()
      case "clinicalTrials" =>
        logger.info("run step clinicaltrials")
        ClinicalTrials()
      case "evidenceDrug" =>
        logger.info("run step evidenceDrug")
        EvidenceDrug()
      case "evidenceDrugDirect" =>
        logger.info("run step evidenceDrug")
        EvidenceDrugDirect()
      case "evidenceProteinFix" =>
        logger.info("run step evidenceProteinFix")
        EvidenceProteinFix()
      case "expression" =>
        logger.info("run step expression")
        Expression()
      case "disease" =>
        logger.info("run step disease")
        Disease()
      case "target" =>
        logger.info("run step target")
        Target()
      case "mousePhenotypes" =>
        logger.info("run step mousephenotypes")
        MousePhenotypes()
      case "reactome" =>
        logger.info("run step reactome (rea)")
        Reactome()
      case "eco" =>
        logger.info("run step eco")
        Eco()
      case "drug" =>
        logger.info("run step drug")
        Drug()
      case "cancerBiomarkers" =>
        logger.info("run step cancerBiomarkers")
        CancerBiomarkers()
      case "dailymed" =>
        logger.info("run step dailymed")
        Dailymed()
      case "ddr" =>
        logger.info("run step dataDrivenRelation")
        DataDrivenRelation()
    }
  }

  def apply(steps: Seq[String]) = {

    ETLSessionContext() match {
      case Right(otContext) =>
        implicit val ctxt = otContext

        logger.debug(ctxt.configuration.toString)

        val etlSteps =
          if (steps.isEmpty) otContext.configuration.common.defaultSteps
          else steps

        val unknownSteps = etlSteps.toSet diff otContext.configuration.common.defaultSteps.toSet
        val knownSteps = etlSteps.toSet intersect otContext.configuration.common.defaultSteps.toSet

        logger.info(s"valid steps to execute: ${knownSteps.toString}")
        logger.warn(s"invalid steps to skip: ${unknownSteps.toString}")

        knownSteps.foreach {
          case step =>
            logger.debug(s"step to run: '${step}'")
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
