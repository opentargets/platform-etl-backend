package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import io.opentargets.etl.backend._

object ETL extends LazyLogging {
  //To add when config is filled
  // "dailymed","associationsLLR","associations"
  // "clinicalTrials",
  val allSteps: Vector[String] = Vector(
    "disease",
    "target",
    "reactome",
    "eco",
    "drug",
    "cancerBiomarkers",
    "search",
    "evidenceDrugDirect",
    "ddr"
  )

  def applySingleStep(step: String, otc: Config)(implicit ss: SparkSession) = {
    import ss.implicits._

    step match {
      case "search" =>
        logger.info("run step search")
        Search(otc)
      case "associations" =>
        logger.info("run step associations")
        Associations(otc)
      case "associationsLLR" =>
        logger.info("run step associations-llr")
        AssociationsLLR(otc)
      case "clinicalTrials" =>
        logger.info("run step clinicaltrials")
        ClinicalTrials(otc)
      case "evidenceDrug" =>
        logger.info("run step evidenceDrug")
        EvidenceDrug(otc)
      case "evidenceDrugDirect" =>
        logger.info("run step evidenceDrug")
        EvidenceDrugDirect(otc)
      case "evidenceProteinFix" =>
        logger.info("run step evidenceProteinFix")
        EvidenceProteinFix(otc)
      case "disease" =>
        logger.info("run step disease")
        Disease(otc)
      case "target" =>
        logger.info("run step target")
        Target(otc)
      case "reactome" =>
        logger.info("run step reactome (rea)")
        Reactome(otc)
      case "eco" =>
        logger.info("run step eco")
        Eco(otc)
      case "drug" =>
        logger.info("run step drug")
        Drug(otc)
      case "cancerBiomarkers" =>
        logger.info("run step cancerBiomarkers")
        CancerBiomarkers(otc)
      case "dailymed" =>
        logger.info("run step dailymed")
        Dailymed(otc)
      case "ddr" =>
        logger.info("run step dataDrivenRelation")
        DataDrivenRelation(otc)
      case _ =>
        logger.error("Exit with error or ALL by defaul (?) ")
    }
  }

  def apply(step: String) = {
    val otc = Configuration.load
    implicit val ss = SparkSessionWrapper.session

    step match {
      case "all" =>
        logger.info("=== Run ALL steps ===")
        for (singleStep <- allSteps) {
          ETL.applySingleStep(singleStep, otc)
        }
      case _ =>
        ETL.applySingleStep(step, otc)
    }
  }

}

object Main {
  def main(args: Array[String]): Unit = {
    val step = if (args.length == 0) "all" else args(0)
    ETL(step)
  }
}
