package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.SparkSession

object Pharmacogenomics extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Executing Pharmacogenomics step.")

    logger.debug("Reading Pharmacogenomics input")
    val mappedInputs = Map("pgx" -> context.configuration.pharmacogenomics.inputs)

    logger.debug("Processing Pharmacogenomics data")
    val inputDataFrames = IoHelpers.readFrom(mappedInputs)
    val pgxDF = inputDataFrames("pgx").data

    logger.debug("Writing Pharmacogenomics outputs")
    val dataframesToSave: IOResources = Map(
      "pgx" -> IOResource(pgxDF, context.configuration.pharmacogenomics.outputs)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

}
