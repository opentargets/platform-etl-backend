package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SearchEBI extends LazyLogging {

  def generateDatasets(resources: IoHelpers.IOResources): Map[String, DataFrame] = {

    val diseases = resources("disease").data.withColumnRenamed("id", "diseaseId")
    val targets = resources("target").data.withColumnRenamed("id", "targetId")
    val associationsDirectOverall = resources("association").data
    val evidence = resources("evidence").data

    val datasetAssociations = associationsDirectOverall
      .join(targets, Seq("targetId"), "inner")
      .join(diseases, Seq("diseaseId"), "inner")
      .select(
        col("targetId"),
        col("diseaseId"),
        col("approvedSymbol"),
        col("name"),
        col("associationScore").alias("score")
      )

    val datasetEvidence = evidence
      .join(targets, Seq("targetId"), "inner")
      .join(diseases, Seq("diseaseId"), "inner")
      .select(
        col("targetId"),
        col("diseaseId"),
        col("approvedSymbol"),
        col("name"),
        col("associationScore").alias("score")
      )
    Map(
      "ebisearchAssociations" -> datasetAssociations,
      "ebisearchEvidence" -> datasetEvidence
    )

  }
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss: SparkSession = context.sparkSession
    val config = context.configuration.steps.searchEbi

    logger.info("Generate EBI Search dataset")
    logger.info("Loading raw inputs for Base Expression step.")

    val inputDataFrames = IoHelpers.readFrom(config.input)
    val dataToSave = generateDatasets(inputDataFrames)

    IoHelpers.writeTo(
      Map(
        "ebisearchEvidence" -> IOResource(
          dataToSave("ebisearchEvidence"),
          config.output("evidence")
        ),
        "ebisearchAssociations" -> IOResource(
          dataToSave("ebisearchAssociations"),
          config.output("associations")
        )
      )
    )
  }
}
