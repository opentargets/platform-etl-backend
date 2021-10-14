package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.{ETLSessionContext, MeddraLowLevelTermsData, MeddraPreferredTermsData}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, typedLit}
import org.apache.spark.storage.StorageLevel

/**
  * This substep of OpenFDA, computes the LLR for targets data.
  * It works on the pre-cooked FAERS dataset (trimmed, filtered events, drug-augmented...) to produce the Montecarlo
  * analysis on targets information.
  */
object OpenFdaTargets {


  /**
    * Compute OpenFDA Targets substep
    * @param dfsData IOResources with source data
    * @param fdaDataFilteredWithDrug OpenFDA FAERS cooked data, including drug data
    * @param context the ETL session context
    * @return It produces two result datasets: filtered and unfiltered openfda llr analysis on targets data
    */
  def apply(dfsData: IOResources, fdaDataFilteredWithDrug: DataFrame)(implicit context: ETLSessionContext) = {
    implicit val sparkSession = context.sparkSession
    import sparkSession.implicits._

    // We'll use only those reports with associated target information
    val fdaDataTargets = fdaDataFilteredWithDrug
      .where($"linkedTargets".isNotNull)
      .withColumn("targetId", explode($"linkedTargets.rows"))
      .drop($"linkedTargets")
    // Prepare the summary statistics used by Montecarlo
    val fdaDataTargetsWithSummaryStats = TargetPrepareSummaryStatistics(fdaDataTargets)
    // Compute the Montecarlo input parameters
    val fdaDataTargetsMontecarloReady = TargetPrepareForMontecarlo(fdaDataTargetsWithSummaryStats)
    // TODO - Attach meddra information
    val fdaDataTargetsMontecarloReadyWithMeddra = fdaDataTargetsMontecarloReady
    // TODO - Do a Stratified Sampling
    // TODO - Run Montecarlo
    val montecarloResults = MonteCarloSampling(
      fdaDataTargetsMontecarloReadyWithMeddra,
      context.configuration.openfda.montecarlo.percentile,
      context.configuration.openfda.montecarlo.permutations
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // Write montecarlo results and unfiltered results
    val outputMap: IOResources = Map(
      "openfdaTargetsUnfiltered" -> IOResource(fdaDataTargetsMontecarloReadyWithMeddra, context.configuration.openfda.outputs.fdaTargetsUnfiltered),
      "openfdaTargetsResults" -> IOResource(montecarloResults, context.configuration.openfda.outputs.fdaTargetsResults)
    )
    IoHelpers.writeTo(outputMap)
  }

}
