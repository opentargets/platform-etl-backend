package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, col}

object TargetPrepareSummaryStatistics {
  /**
    * This method will compute the number of reports per reaction, the number of reports per the target context, in this
    * case 'targetId', and the number of reports on a given reaction for every target context, i.e. (reaction, targetId)
    *
    * @param fdaData the FAERS data with target IDs
    * @param context application context
    * @return FAERS reports with ChEMBL, target context (target IDs), reaction meddra information if provided and the
    *         aggregated computed values
    */
  def apply(fdaData: DataFrame)(implicit context: ETLSessionContext) = {
    // Define the partition windows
    val wAdverses = Window.partitionBy(col("reaction_reactionmeddrapt"))
    val wTargets = Window.partitionBy(col("targetId"))
    val wAdverseTargetComb =
      Window.partitionBy(col("targetId"), col("reaction_reactionmeddrapt"))
    // and we will need this processed data later on
    val summaryStats = fdaData
      .withColumn("uniq_report_ids_by_reaction", // how many reports mention that reaction
        approx_count_distinct(col("safetyreportid")).over(wAdverses))
      .withColumn("uniq_report_ids_by_target", // how many reports mention that drug
        approx_count_distinct(col("safetyreportid")).over(wTargets))
      .withColumn("uniq_report_ids", // how many mentions of drug-reaction pair
        approx_count_distinct(col("safetyreportid")).over(wAdverseTargetComb))
      .select(
        "safetyreportid",
        "chembl_id",
        "targetId",
        "reaction_reactionmeddrapt",
        "uniq_report_ids_by_reaction",
        "uniq_report_ids_by_target",
        "uniq_report_ids"
      )
    summaryStats
  }
}
