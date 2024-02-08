package io.opentargets.etl.backend.pharmacogenomics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.common.DrugUtils.MapDrugId
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Pharmacogenomics extends LazyLogging {

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

    logger.debug("Map drug id from molecule")
    val mappedDF = MapDrugId(pgxDF, drugDF)

    logger.debug("Get Target Lut")
    val drugTargetLutDF = getDrugTargetLut(drugDF)

    logger.debug("Get Data Enriched")
    val dataEnrichedDF =
      mappedDF
        .join(drugTargetLutDF, Seq("drugId"), "left")
        .drop("drugTargetIds")
        .distinct()

    logger.debug("Writing Pharmacogenomics outputs")
    val dataframesToSave: IOResources = Map(
      "pgx" -> IOResource(dataEnrichedDF, context.configuration.pharmacogenomics.outputs)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

}
