import $file.backend.common
import common._
import $file.backend.disease
import disease._
import $file.backend.target
import target._
import $file.backend.reactome
import reactome._
import $file.backend.eco
import eco._
import $file.backend.drug
import drug._
import $file.backend.cancerBiomarkers
import cancerBiomarkers._
import $file.backend.dataDrivenRelation
import dataDrivenRelation._
import $file.backend.associations
import associations._
import $file.backend.associationsLLR
import associationsLLR._

import $file.backend.clinicalTrials
import clinicalTrials._

import $file.backend.dailymed
import dailymed._
import $file.backend.evidenceDrug
import evidenceDrug._
import $file.backend.evidenceDrugDirect
import evidenceDrugDirect._

import $file.backend.evidenceProteinFix
import evidenceProteinFix._

import $file.backend.search
import search._

import com.typesafe.scalalogging.LazyLogging

object ETL extends LazyLogging {
  def apply(step: String) = {
    val otc = Configuration.load

    implicit val spark = SparkSessionWrapper.session

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
}

/**
  Read by default the conf file amm.application.conf and it generates all the indexes.
  step: disease, target, drug
  */
@main
def main(step: String = ""): Unit = ETL(step)
