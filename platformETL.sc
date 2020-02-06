import $file.backend.common
import common._
import $file.backend.disease
import disease._
import $file.backend.target
import target._
import $file.backend.drug
import drug._
import $file.backend.associations
import associations._

import $file.backend.clinicalTrials
import clinicalTrials._

import $file.backend.dailymed
import dailymed._

import $file.backend.evidenceProteinFix
import evidenceProteinFix._

import $file.backend.evidenceGWASFix
import evidenceGWASFix._

import $file.backend.search
import search._
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`

import com.typesafe.scalalogging.LazyLogging

object ETL extends LazyLogging {
  def apply(step: String) = {
    val otc = Configuration.load

    //  val cfg = getConfig(conf)
    //  val listInputFiles = getInputFiles(cfg, step)
    //  val outputPathPrefix = cfg.getString("output-dir")

    implicit val spark = SparkSessionWrapper.session

    step match {
      case "search" =>
        logger.info("run step search")
        Search(otc)
      case "associations" =>
        logger.info("run step associations")
        Associations(otc)
      case "clinicalTrials" =>
        logger.info("run step clinicaltrials")
        ClinicalTrials(otc)
      case "evidenceProteinFix" =>
        logger.info("run step evidenceProteinFix")
        EvidenceProteinFix(otc)
      case "evidenceGWASFix" =>
        logger.info("run step evidenceGWASFix")
        EvidenceGWASFix(otc)
      case "disease" =>
        logger.info("run step disease")
        Disease(otc)
      case "target" =>
        logger.info("run step target")
        Target(otc)
      case "drug" =>
        logger.info("run step drug")
        Drug(otc)
      case "dailymed" =>
        logger.info("run step dailymed")
        Dailymed(otc)
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
