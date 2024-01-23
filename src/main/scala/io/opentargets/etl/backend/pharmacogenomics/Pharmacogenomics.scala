package io.opentargets.etl.backend.pharmacogenomics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Pharmacogenomics extends LazyLogging {

  private def getChebiChemblLut(drugs: DataFrame): DataFrame =
    drugs
      .select(col("id"), explode(col("crossReferences")))
      .filter(col("key") === "chEBI")
      .withColumn("drugId", explode(col("value")))
      .select(
        col("id").alias("drugId"),
        concat(lit("CHEBI_"), col("drugId")).alias("drugFromSourceId")
      )
      .distinct()

  private def getDrugTargetLut(drugs: DataFrame): DataFrame =
    drugs
      .filter(
        (col("linkedTargets").isNotNull()) && (size(col("linkedTargets.rows")) >= 1)
      )
      .select(
        col("id").alias("drugId"),
        col("linkedTargets.rows").alias("drugTargetIds")
      )

  private def getHighConfidenceData(data: DataFrame): DataFrame =
    data.filter(col("evidenceLevel").isin(Seq("1A", "1B", "2A", "2B"): _*))

  private def flagIsDirectTarget(variant_target: Column, drug_targets: Column): Column =
    when(array_contains(drug_targets, variant_target), true).otherwise(false)

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Executing Pharmacogenomics step.")

    logger.debug("Reading Pharmacogenomics input")
    val mappedInputs = Map(
      "pgx" -> context.configuration.pharmacogenomics.inputs.pgkb,
      "drug" -> context.configuration.pharmacogenomics.inputs.drug,
      "diseases" -> context.configuration.pharmacogenomics.inputs.diseases
    )

    logger.debug("Processing Pharmacogenomics data")
    val inputDataFrames = IoHelpers.readFrom(mappedInputs)
    val pgxDF = inputDataFrames("pgx").data
    val drugDF = inputDataFrames("drug").data

    val xrefs = getChebiChemblLut(drugDF)
    val drug_target_lut = getDrugTargetLut(drugDF)

    val dataEnrichedDF = (
      pgxDF
        .withColumnRenamed("drugId", "drugFromSourceId")
        .join(xrefs, Seq("drugFromSourceId"), "left")
        .join(drug_target_lut, Seq("drugId"), "left")
        .withColumn(
          "isDirectTarget",
          flagIsDirectTarget(col("targetFromSourceId"), col("drugTargetIds"))
        )
        .drop("drugTargetIds")
        .distinct()
        .persist()
    )

    val hcDF = getHighConfidenceData(dataEnrichedDF)

    logger.debug("Writing Pharmacogenomics outputs")
    val dataframesToSave: IOResources = Map(
      "pgx" -> IOResource(hcDF, context.configuration.pharmacogenomics.outputs)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

}
