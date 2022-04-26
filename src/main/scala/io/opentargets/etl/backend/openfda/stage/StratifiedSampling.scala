package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.Helpers.IOResourceConfig
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame

object StratifiedSampling extends LazyLogging {

  /** @param cleanFda       cleanFda is the data after it has been cleaned: filtered by blacklist, qualifications of reporter, patient death, etc.
    * @param significantFda significantFda is the data that has been prepared for MC sampling, as at this point we have already removed all log-likelihood rations that are effectively zero.
    * @param sampleSize proportion of dataset to take
    */
  def apply(
      rawFda: DataFrame,
      cleanFda: DataFrame,
      significantFda: DataFrame,
      targetDimensionColId: String,
      sampleSize: Double = 0.1
  )(implicit context: ETLSessionContext): IOResources = {
    import org.apache.spark.sql.functions._

    logger.debug(s"Generating Stratified Sampling for target dimension '${targetDimensionColId}'")

    val significantSubsetOnTargetDimension =
      significantFda.select(targetDimensionColId).distinct.sample(sampleSize)
    val allOnTargetDimension = cleanFda.select(targetDimensionColId).distinct.sample(sampleSize)

    val sampleOnTargetDimension: DataFrame =
      significantSubsetOnTargetDimension
        .join(allOnTargetDimension, Seq(targetDimensionColId), "full_outer")
        .distinct

    // this leaves us with the report ids from the original data
    logger.debug(s"Converting target dimension '${targetDimensionColId}' to safetyreportid")
    val reportIds = cleanFda
      .select(targetDimensionColId, "safetyreportid")
      .join(sampleOnTargetDimension, Seq(targetDimensionColId))
      .drop(targetDimensionColId)
      .distinct()

    logger.info(s"Writing statified sampling for target dimension '${targetDimensionColId}'...")
    // Write Stratified Sampling information
    //val writeMap: IOResources =
    //IoHelpers.writeTo(writeMap)
    Map(
      s"stratifiedSampling_${targetDimensionColId}" -> IOResource(
        rawFda
          .withColumn("seriousnessdeath", lit(1))
          .join(reportIds, Seq("safetyreportid")),
        context.configuration.openfda.outputs.sampling
      )
    )

  }

}
