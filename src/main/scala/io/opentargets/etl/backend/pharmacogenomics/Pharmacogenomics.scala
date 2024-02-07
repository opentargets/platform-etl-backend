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
      .select(col("id"), lower(col("name")).as("drugFromSource"), explode(col("crossReferences")))
      .filter(col("key") === "chEBI")
      .withColumn("drugId", explode(col("value")))
      .select(
        col("id").alias("drugIdCross"),
        col("drugFromSource"),
        concat(lit("CHEBI_"), col("drugId")).alias("drugFromSourceId")
      )
      .distinct()

  private def getDrugTargetLut(drugs: DataFrame): DataFrame =
    drugs
      .filter(
        col("linkedTargets").isNotNull
          && size(col("linkedTargets.rows")) >= 1
      ) select (
      col("id").alias("drugId"),
      col("linkedTargets.rows").alias("drugTargetIds")
    )

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Executing Pharmacogenomics step.")

    logger.debug("Reading Pharmacogenomics inputs")
    val mappedInputs = Map(
      "pgx" -> context.configuration.pharmacogenomics.inputs.pgkb,
      "drug" -> context.configuration.pharmacogenomics.inputs.drug
    )

    logger.debug("Processing Pharmacogenomics data")
    val inputDataFrames = IoHelpers.readFrom(mappedInputs)
    val pgxDF = inputDataFrames("pgx").data
    val drugDF = inputDataFrames("drug").data

    logger.debug("Get Chebi Chembl Lut")
    val xrefsDF = getChebiChemblLut(drugDF)

    val drugNameDF = pgxDF
      .withColumn("drugFromSource", lower(col("drugFromSource")))
      .join(xrefsDF.select("drugFromSource", "drugIdCross"), Seq("drugFromSource"), "left")
    val nonResolvedByNameDF =
      drugNameDF.where(col("drugIdCross").isNull && col("drugFromSourceId").isNotNull)

    val mergedByChebiDF = nonResolvedByNameDF.join(
      xrefsDF.select(col("drugFromSourceId"), col("drugIdCross").as("drugIdCrossChebi")),
      Seq("drugFromSourceId"),
      "left"
    )

    val resolvedByNameDF = drugNameDF
      .where(col("drugIdCross").isNotNull || col("drugFromSourceId").isNull)
      .select(col("*"), lit(null).as("drugIdCrossChebi"))

    val fullDF = resolvedByNameDF.union(mergedByChebiDF)

    val withDrugIdDF =
      fullDF
        .select(col("*"), coalesce(col("drugIdCross"), col("drugIdCrossChebi")).as("drugId"))
        .drop("drugIdCrossChebi", "drugIdCross")

    logger.debug("Get Target Lut")
    val drugTargetLutDF = getDrugTargetLut(drugDF)

    logger.debug("Get Data Enriched")
//    val dataEnrichedDF =
//      pgxDF
//        .withColumnRenamed("drugId", "drugFromSourceId")
//        .join(xrefsDF, Seq("drugFromSourceId"), "left")
//        .join(drugTargetLutDF, Seq("drugId"), "left")
//        .drop("drugTargetIds")
//        .distinct()

    logger.debug("Writing Pharmacogenomics outputs")
    val dataframesToSave: IOResources = Map(
      "pgx" -> IOResource(pgxDF, context.configuration.pharmacogenomics.outputs)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

}
