package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.openfda.stage.StratifiedSampling.logger
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame

object TargetStratifiedSampling extends LazyLogging {

  def apply(rawFda: DataFrame, cleanFda: DataFrame, significantFda: DataFrame, targetDimension: String, sampleSize: Double = 0.1)(
    implicit context: ETLSessionContext): Unit = {
    import org.apache.spark.sql.functions._

    logger.debug("Generating data for sample")

    val significantSample = significantFda.select(targetDimension).distinct.sample(sampleSize)
    val allSample = cleanFda.select(targetDimension).distinct.sample(sampleSize)

    val sampleOfData: DataFrame =
      significantSample.join(allSample, Seq(targetDimension), "full_outer").distinct

    // this leaves us with the report ids from the original data
    logger.debug("Converting dimension to safetyreportid")
    val reportIds = cleanFda
      .select(targetDimension, "safetyreportid")
      .join(sampleOfData, Seq(targetDimension))
      .drop(targetDimension)
      .distinct()

    logger.info("Writing statified...")
    // Write Stratified Sampling information
    val writeMap: IOResources = Map(
      "targetStratifiedSampling" -> IOResource(
        rawFda
          .withColumn("seriousnessdeath", lit(1))
          .join(reportIds, Seq("safetyreportid")),
        context.configuration.openfda.outputs.samplingTargets
      )
    )
    IoHelpers.writeTo(writeMap)
  }
}
