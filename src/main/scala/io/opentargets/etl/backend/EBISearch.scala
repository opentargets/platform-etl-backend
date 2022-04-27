package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.SparkSession

object EBISearch extends LazyLogging {

  def generateDatasets(resources: IoHelpers.IOResources): Map[String, DataFrame] = {

    val diseases = resources("diseases").data.withColumnRenamed("id", "diseaseId")
    val targets = resources("targets").data.withColumnRenamed("id", "targetId")
    val associationsDirectOverall = resources("associationDirectOverall").data
    val evidence = resources("evidence").data

    val datasetAssociations = associationsDirectOverall
      .join(targets, Seq("targetId"), "inner")
      .join(diseases, Seq("diseaseId"), "inner")
      .select("targetId", "diseaseId", "approvedSymbol", "name", "score")

    val datasetEvidence = evidence
      .join(targets, Seq("targetId"), "inner")
      .join(diseases, Seq("diseaseId"), "inner")
      .select("targetId", "diseaseId", "approvedSymbol", "name", "score")

    Map(
      "ebisearchAssociations" -> datasetAssociations,
      "ebisearchEvidence" -> datasetEvidence
    )

  }
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Generate EBI Search dataset")

    val EBIConfiguration = context.configuration.ebisearch

    logger.info("Loading raw inputs for Base Expression step.")

    val mappedInputs = Map(
      "diseases" -> EBIConfiguration.diseaseEtl,
      "targets" -> EBIConfiguration.targetEtl,
      "evidence" -> EBIConfiguration.evidenceETL,
      "associationDirectOverall" -> EBIConfiguration.associationETL
    )

    val inputDataFrames = IoHelpers.readFrom(mappedInputs)
    val dataToSave = generateDatasets(inputDataFrames)

    logger.info(s"write to ${context.configuration.common.output}/ebisearch")
    IoHelpers.writeTo(
      Map(
        "ebisearchEvidence" -> IOResource(
          dataToSave("ebisearchEvidence"),
          EBIConfiguration.outputs.ebisearchEvidence
        ),
        "ebisearchAssociations" -> IOResource(
          dataToSave("ebisearchAssociations"),
          EBIConfiguration.outputs.ebisearchAssociations
        )
      )
    )
  }
}
