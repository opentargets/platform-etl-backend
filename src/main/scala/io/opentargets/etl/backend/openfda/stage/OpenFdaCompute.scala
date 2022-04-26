package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.{
  ETLSessionContext,
  FdaData,
  MeddraLowLevelTermsData,
  MeddraPreferredTermsData,
  TargetDimension
}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.storage.StorageLevel

object OpenFdaCompute extends LazyLogging {
  def apply(dfsData: IOResources, fdaCookedData: DataFrame, targetDimension: TargetDimension)(
      implicit context: ETLSessionContext
  ): IOResources = {
    implicit val sparkSession = context.sparkSession

    // Prepare Summary Statistics
    val fdaDataWithSummaryStats =
      PrepareSummaryStatistics(fdaCookedData, targetDimension.colId, targetDimension.statsColId)
    // Montecarlo data preparation
    val fdaDataMontecarloReady =
      PrepareForMontecarlo(fdaDataWithSummaryStats, targetDimension.statsColId)
    // Add Meddra
    val fdaDataWithMeddra = (context.configuration.openfda.meddra match {
      case Some(_) =>
        AttachMeddraData(
          fdaDataMontecarloReady,
          targetDimension.colId,
          dfsData(MeddraPreferredTermsData()).data,
          dfsData(MeddraLowLevelTermsData()).data
        )
      case _ =>
        fdaDataMontecarloReady
          .withColumn("meddraCode", typedLit[String](""))
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // Conditional generation of Stratified Sampling
    val stratifiedSamplingData: IOResources = if (context.configuration.openfda.sampling.enabled) {
      // This one really uses the raw OpenFDA Data
      StratifiedSampling(
        dfsData(FdaData()).data,
        fdaDataWithSummaryStats,
        fdaDataWithMeddra,
        targetDimension.colId
      )
    } else Map()
    // Compute Montecarlo Sampling
    val montecarloResults = MonteCarloSampling(
      fdaDataWithMeddra,
      targetDimension.colId,
      targetDimension.statsColId,
      context.configuration.openfda.montecarlo.percentile,
      context.configuration.openfda.montecarlo.permutations
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // Produce Output
    logger.info(s"Write OpenFDA computation for target dimension '${targetDimension.colId}'")
    val outputMap: IOResources = Map(
      s"unfiltered-${targetDimension.colId}" -> IOResource(
        fdaDataWithMeddra,
        targetDimension.outputUnfilteredResults
      ),
      s"openFdaResults-${targetDimension.colId}" -> IOResource(
        montecarloResults,
        targetDimension.outputResults
      )
    ) ++ stratifiedSamplingData
    IoHelpers.writeTo(outputMap)
  }
}
