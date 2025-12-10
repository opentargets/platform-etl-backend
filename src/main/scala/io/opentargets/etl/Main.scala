package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging

import scala.util._
import io.opentargets.etl.backend._
import io.opentargets.etl.backend.target.Target
import io.opentargets.etl.backend.drug.Drug

import io.opentargets.etl.backend.literature.Literature
import io.opentargets.etl.backend.searchFacet.FacetSearch
import io.opentargets.etl.backend.pharmacogenomics.Pharmacogenomics
import io.opentargets.etl.backend.targetEngine.TargetEngine

object ETL extends LazyLogging {
  def applySingleStep(step: String)(implicit context: ETLSessionContext): Unit = {
    logger.info(s"running step $step")
    step.toLowerCase match {
      case "association"      => Association()
      case "association_otf"  => AssociationOTF()
      case "drug"             => Drug()
      case "expression"       => Expression()
      case "openfda"          => OpenFda()
      case "go"               => Go()
      case "interaction"      => Interactions()
      case "known_drug"       => KnownDrugs()
      case "literature"       => Literature()
      case "otar"             => Otar()
      case "pharmacogenomics" => Pharmacogenomics()
      case "reactome"         => Reactome()
      case "search"           => Search()
      case "search_ebi"       => SearchEBI()
      case "search_facet"     => FacetSearch()
      case "target"           => Target()
      case "target_engine"    => TargetEngine()
      case "mouse_phenotype"  => MousePhenotype()
      case _                  => throw new IllegalArgumentException(s"step $step is unknown")
    }
    logger.info(s"finished running step $step")
  }

  def apply(steps: Seq[String]): Unit =
    ETLSessionContext() match {
      case Right(otContext) =>
        implicit val ctxt: ETLSessionContext = otContext
        steps.foreach(step => ETL.applySingleStep(step))
      case Left(ex) =>
        logger.error(ex.prettyPrint())
    }
}

object Main {
  def main(args: Array[String]): Unit = ETL(args)
}
