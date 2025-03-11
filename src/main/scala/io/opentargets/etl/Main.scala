package io.opentargets.etl

import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import com.google.cloud.storage.Storage.BlobListOption
import com.typesafe.scalalogging.LazyLogging

import scala.util._
import io.opentargets.etl.backend._
import io.opentargets.etl.backend.target.Target
import io.opentargets.etl.backend.drug.Drug
import io.opentargets.etl.backend.evidence.Evidence
import io.opentargets.etl.backend.graph.EtlDag
import io.opentargets.etl.backend.literature.{Epmc, Literature}
import io.opentargets.etl.backend.facetSearch.FacetSearch
import io.opentargets.etl.backend.pharmacogenomics.Pharmacogenomics
import io.opentargets.etl.backend.targetEngine.TargetEngine
import io.opentargets.etl.common.GoogleStorageHelpers

import scala.collection.JavaConverters

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
      case "mouse_phenotypes" => MousePhenotypes()
      case _                  => throw new IllegalArgumentException(s"step $step is unknown")
    }
    logger.info(s"finished running step $step")
  }

  /** Identify steps whose outputs already exist in GCP.
    */
  def stepsWithExistingOuputs(implicit ctx: ETLSessionContext): Set[String] = {
    lazy val outputPaths: Map[String, String] = Map(
      "association" -> ctx.configuration.associations.outputs.directByDatatype.path,
      "association_otf" -> ctx.configuration.aotf.outputs.clickhouse.path,
      "disease" -> ctx.configuration.disease.outputs.diseases.path,
      "drug" -> ctx.configuration.drug.outputs.drug.path,
      "evidence" -> ctx.configuration.evidences.outputs.succeeded.path,
      "expression" -> ctx.configuration.expression.output.path,
      "openfda" -> ctx.configuration.openfda.outputs.fdaResults.path,
      "go" -> ctx.configuration.geneOntology.output.path,
      "interaction" -> ctx.configuration.interactions.outputs.interactions.path,
      "known_drug" -> ctx.configuration.knownDrugs.output.path,
      "literature" -> ctx.configuration.literature.processing.outputs.literatureIndex.path,
      "pharmacogenomics" -> ctx.configuration.pharmacogenomics.outputs.path,
      "reactome" -> ctx.configuration.reactome.output.path,
      "search" -> ctx.configuration.search.outputs.diseases.path,
      "search_ebi" -> ctx.configuration.ebisearch.outputs.ebisearchEvidence.path,
      "search_facet" -> ctx.configuration.facetSearch.outputs.targets.path,
      "target" -> ctx.configuration.target.outputs.target.path,
      "target_engine" -> ctx.configuration.targetEngine.outputs.targetEngine.path,
      "mouse_phenotypes" -> ctx.configuration.mousePhenotypes.output.succeeded.path
    )

    val storage: Storage = StorageOptions.getDefaultInstance.getService

    val (bucket, blob) = GoogleStorageHelpers.pathToBucketBlob(ctx.configuration.common.path)

    val blobs: Page[Blob] =
      storage.list(bucket, BlobListOption.currentDirectory(), BlobListOption.prefix(blob))

    val existingOutputs: Set[String] = JavaConverters
      .asScalaIteratorConverter(blobs.iterateAll().iterator())
      .asScala
      .filter(_.isDirectory)
      .map(dir => s"gs://${bucket}/${dir.getName}".dropRight(1))
      .toSet
    logger.debug(existingOutputs.toString)
    outputPaths.filter(kv => existingOutputs.contains(kv._2)).keySet
  }

  def apply(steps: Seq[String]): Unit =
    ETLSessionContext() match {
      case Right(otContext) =>
        implicit val ctxt: ETLSessionContext = otContext

        val completedSteps =
          if (
            otContext.configuration.sparkSettings.ignoreIfExists && otContext.configuration.sparkSettings.writeMode == "ignore"
          )
            stepsWithExistingOuputs
          else Set.empty[String]
        logger.info(s"Steps already completed and not to be executed again: $completedSteps")
        // build ETL DAG graph
        val etlDag = new EtlDag[String](otContext.configuration.etlDag.steps)

        val etlSteps =
          if (steps.isEmpty) etlDag.getAll
          else if (otContext.configuration.etlDag.resolve) etlDag.getDependenciesFor(steps: _*)
          else steps

        logger.info(s"Steps to execute: $etlSteps")

        val filteredSteps = etlSteps.filter(s => !completedSteps.contains(s))
        logger.info(s"Steps to execute after filtering: $filteredSteps")

        filteredSteps.foreach { step =>
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
