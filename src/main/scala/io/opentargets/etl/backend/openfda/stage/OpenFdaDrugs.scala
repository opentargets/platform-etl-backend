package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.{ETLSessionContext, FdaData, MeddraLowLevelTermsData, MeddraPreferredTermsData}
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.storage.StorageLevel

object OpenFdaDrugs {

  /**
    * Compute OpenFDA FAERS analysis over Drug data
    * @param dfsData IOResources with source data
    * @param fdaDataFilteredWithDrug OpenFDA FAERS cooked data, including drug information
    * @param context ETL session context
    * @return It produces two result datasets: filtered and unfiltered openfda llr analysis
    */
  def apply(dfsData: IOResources, fdaDataFilteredWithDrug: DataFrame)(implicit context: ETLSessionContext) = {
    // Target Dimension
    val targetDimensionColId = "chembl_id"
    val targetDimensionStatsColId = "uniq_report_ids_by_drug"

    // Prepare Summary Statistics
    val fdaDataWithSummaryStats = PrepareSummaryStatistics(fdaDataFilteredWithDrug, targetDimensionColId, targetDimensionStatsColId)
    // Montecarlo data preparation, for drugs
    val fdaDataMontecarloReady = PrepareForMontecarlo(fdaDataWithSummaryStats, targetDimensionStatsColId)
    // Add Meddra
    val fdaDataWithMeddra = context.configuration.openfda.meddra match {
      case Some(_) => AttachMeddraData(fdaDataMontecarloReady,
        targetDimensionColId,
        dfsData(MeddraPreferredTermsData()).data,
        dfsData(MeddraLowLevelTermsData()).data).persist(StorageLevel.MEMORY_AND_DISK_SER)
      case _ => fdaDataMontecarloReady
        .withColumn("meddraCode", typedLit[String](""))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    // Conditional generation of Stratified Sampling
    if (context.configuration.openfda.sampling.enabled) {
      // This one really uses the raw OpenFDA Data
      StratifiedSampling(dfsData(FdaData()).data, fdaDataWithSummaryStats, fdaDataWithMeddra, targetDimensionColId)
    }
    // Compute Montecarlo Sampling - For Drugs
    val montecarloResults = MonteCarloSampling(
      fdaDataWithMeddra,
      targetDimensionColId,
      targetDimensionStatsColId,
      context.configuration.openfda.montecarlo.percentile,
      context.configuration.openfda.montecarlo.permutations
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // Produce Output
    val outputMap: IOResources = Map(
      "unfiltered" -> IOResource(fdaDataWithMeddra, context.configuration.openfda.outputs.fdaUnfiltered),
      "openFdaResults" -> IOResource(montecarloResults, context.configuration.openfda.outputs.fdaResults)
    )
    IoHelpers.writeTo(outputMap)
  }

}
