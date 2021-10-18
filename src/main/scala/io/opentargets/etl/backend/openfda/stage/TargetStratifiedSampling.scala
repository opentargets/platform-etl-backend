package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.openfda.stage.StratifiedSampling.logger
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame

object TargetStratifiedSampling extends LazyLogging {

  /**
    * @param cleanFda       cleanFda is the data after it has been cleaned: filtered by blacklist, qualifications of reporter, patient death, etc.
    * @param significantFda significantFda is the data that has been prepared for MC sampling, as at this point we have already removed all log-likelihood rations that are effectively zero.
    * @param sampleSize proportion of dataset to take
    */
  def apply(rawFda: DataFrame, cleanFda: DataFrame, significantFda: DataFrame, idCol: String, sampleSize: Double = 0.1)(
    implicit context: ETLSessionContext): Unit = {
    import org.apache.spark.sql.functions._

    logger.debug("Generating data for sample")

    val significantSample = significantFda.select(idCol).distinct.sample(sampleSize)
    val allSample = cleanFda.select(idCol).distinct.sample(sampleSize)

    val sampleOfData: DataFrame =
      significantSample.join(allSample, Seq(idCol), "full_outer").distinct

    // this leaves us with the report ids from the original data
    logger.debug("Converting dimension to safetyreportid")
    val reportIds = cleanFda
      .select(idCol, "safetyreportid")
      .join(sampleOfData, Seq(idCol))
      .drop(idCol)
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
