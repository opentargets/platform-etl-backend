package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.SparkSession

object EBISearch extends LazyLogging {

  def generateDatasets(resources: IoHelpers.IOResources): Map[String, DataFrame] = {

    val diseases = resources("disease").data.withColumnRenamed("id", "diseaseId")
    val targets = resources("target").data.withColumnRenamed("id", "targetId")
    val associationsDirectOverall = resources("association").data
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

    val EBIConfiguration = context.configuration.searchEbi

    logger.info("Loading raw inputs for Base Expression step.")

//    val mappedInputs = Map(
//      "diseases" -> EBIConfiguration.diseaseEtl,
//      "targets" -> EBIConfiguration.targetEtl,
//      "evidence" -> EBIConfiguration.evidenceETL,
//      "associationDirectOverall" -> EBIConfiguration.associationETL
//    )

    val inputDataFrames = IoHelpers.readFrom(EBIConfiguration.input)
    val dataToSave = generateDatasets(inputDataFrames)

    IoHelpers.writeTo(
      Map(
        "ebisearchEvidence" -> IOResource(
          dataToSave("ebisearchEvidence"),
          EBIConfiguration.output("evidence")
        ),
        "ebisearchAssociations" -> IOResource(
          dataToSave("ebisearchAssociations"),
          EBIConfiguration.output("associations")
        )
      )
    )
  }
}
