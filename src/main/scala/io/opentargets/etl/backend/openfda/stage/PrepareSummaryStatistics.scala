package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, col}

object PrepareSummaryStatistics {
  def apply(fdaData: DataFrame)(implicit context: ETLSessionContext) = {

    // Define the partition windows
    val wAdverses = Window.partitionBy(col("reaction_reactionmeddrapt"))
    val wDrugs = Window.partitionBy(col("chembl_id"))
    // TODO - Create a window for target IDs
    val wAdverseDrugComb =
      Window.partitionBy(col("chembl_id"), col("reaction_reactionmeddrapt"))
    // TODO - Create a window for target-reaction combination

    // and we will need this processed data later on
    // TODO - Extend this with 'uniq_report_ids_by_target' and 'uniq_report_ids_targets'
    val groupedDf = fdaData
      .withColumn("uniq_report_ids_by_reaction", // how many reports mention that reaction
        approx_count_distinct(col("safetyreportid")).over(wAdverses))
      .withColumn("uniq_report_ids_by_drug", // how many reports mention that drug
        approx_count_distinct(col("safetyreportid")).over(wDrugs))
      .withColumn("uniq_report_ids", // how many mentions of drug-reaction pair
        approx_count_distinct(col("safetyreportid")).over(wAdverseDrugComb))
      .select(
        "safetyreportid",
        "chembl_id",
        "reaction_reactionmeddrapt",
        "uniq_report_ids_by_reaction",
        "uniq_report_ids_by_drug",
        "uniq_report_ids"
      )
    groupedDf
  }
}
