package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging

import scala.util._
import io.opentargets.etl.backend._
import io.opentargets.etl.backend.target.Target
import io.opentargets.etl.backend.drug.Drug
import io.opentargets.etl.backend.evidence.Evidence
import io.opentargets.etl.backend.graph.EtlDag
import io.opentargets.etl.backend.literature.Literature
import io.opentargets.etl.backend.facetSearch.FacetSearch
import io.opentargets.etl.backend.pharmacogenomics.Pharmacogenomics
import io.opentargets.etl.backend.targetEngine.TargetEngine

object ETL extends LazyLogging {
  def applySingleStep(step: String)(implicit context: ETLSessionContext): Unit = {
    logger.info(s"running step $step")

    step.toLowerCase match {
      case "association"      => Association()
      case "association_otf"  => AssociationOTF()
      case "disease"          => Disease()
      case "drug"             => Drug()
      case "evidence"         => Evidence()
      case "expression"       => Expression()
      case "openfda"          => OpenFda()
      case "go"               => Go()
      case "interaction"      => Interactions()
      case "known_drug"       => KnownDrugs()
      case "literature"       => Literature()
      case "otar"             => OtarProject()
      case "pharmacogenomics" => Pharmacogenomics()
      case "reactome"         => Reactome()
      case "search"           => Search()
      case "search_ebi"       => EBISearch()
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

object Main {
  def main(args: Array[String]): Unit =
    ETL(args)
}
