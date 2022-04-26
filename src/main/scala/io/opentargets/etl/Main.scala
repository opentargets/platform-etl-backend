package io.opentargets.etl

import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import com.google.cloud.storage.Storage.BlobListOption
import com.typesafe.scalalogging.LazyLogging

import scala.util._
import io.opentargets.etl.backend._
import io.opentargets.etl.backend.drug.Drug
import io.opentargets.etl.backend.graph.EtlDag
import io.opentargets.etl.common.GoogleStorageHelpers

import scala.collection.JavaConverters

object ETL extends LazyLogging {

  def applySingleStep(step: String)(implicit context: ETLSessionContext): Unit = {
    step.toLowerCase match {
      case "association" =>
        logger.info("run step association")
        Association()
      case "associationotf" =>
        logger.info("run step associationOTF")
        AssociationOTF()
      case "connection" =>
        logger.info("run step connections")
        Connections()
      case "disease" =>
        logger.info("run step disease")
        Disease()
      case "drug" =>
        logger.info("run step drug")
        Drug()
      case "evidence" =>
        logger.info("run step evidence")
        Evidence()
      case "expression" =>
        logger.info("run step expression")
        Expression()
      case "ebisearch" =>
        logger.info("Running EBI Search step")
        EBISearch()
      case "fda" =>
        logger.info("Running OpenFDA FAERS step")
        OpenFda()
      case "go" =>
        logger.info("run step go")
        Go()
      case "interaction" =>
        logger.info("run step interactions")
        Interactions()
      case "knowndrug" =>
        logger.info("run step knownDrugs")
        KnownDrugs()
      case "otar" =>
        logger.info("Running Otar projects step")
        OtarProject()
      case "reactome" =>
        logger.info("run step reactome (rea)")
        Reactome()
      case "search" =>
        logger.info("run step search")
        Search()
      case "target" =>
        logger.info("run step target")
        target.Target()
      case "targetvalidation" =>
        logger.info("run step targetValidation")
        TargetValidation()
      case _ => logger.warn(s"step $step is unknown so nothing to execute")
    }
    logger.info(s"finished to run step ($step)")
  }

  /** Identify steps whose outputs already exist in GCP.
    */
  def stepsWithExistingOuputs(implicit ctx: ETLSessionContext): Set[String] = {
    lazy val outputPaths: Map[String, String] = Map(
      "disease" -> ctx.configuration.disease.outputs.diseases.path,
      "reactome" -> ctx.configuration.reactome.output.path,
      "expression" -> ctx.configuration.expression.output.path,
      "go" -> ctx.configuration.geneOntology.output.path,
      "target" -> ctx.configuration.target.outputs.target.path,
      "interaction" -> ctx.configuration.interactions.outputs.interactions.path,
      "targetValidation" -> ctx.configuration.targetValidation.output.succeeded.path,
      "evidence" -> ctx.configuration.evidences.outputs.succeeded.path,
      "association" -> ctx.configuration.associations.outputs.directByDatatype.path,
      "associationOTF" -> ctx.configuration.aotf.outputs.clickhouse.path,
      "search" -> ctx.configuration.search.outputs.diseases.path,
      "drug" -> ctx.configuration.drug.outputs.drug.path,
      "knownDrug" -> ctx.configuration.knownDrugs.output.path,
      "ebisearch" -> ctx.configuration.ebisearch.outputs.ebisearchEvidence.path,
      "fda" -> ctx.configuration.openfda.outputs.fdaResults.path
    )

    val storage: Storage = StorageOptions.getDefaultInstance.getService

    val (bucket, blob) = GoogleStorageHelpers.pathToBucketBlob(ctx.configuration.common.output)

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

  def apply(steps: Seq[String]): Unit = {

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
}

object Main {
  def main(args: Array[String]): Unit = {
    ETL(args)
  }
}
