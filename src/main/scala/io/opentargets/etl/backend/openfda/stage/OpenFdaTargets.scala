package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.explode

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
    // TODO - Compute the Montecarlo input parameters
    // TODO - Attach meddra information
    // TODO - Do a Stratified Sampling
    // TODO - Run Montecarlo
    // TODO - Write montecarlo results and unfiltered results

  }

}
