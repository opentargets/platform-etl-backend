package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, col}

object PrepareSummaryStatistics {
  def apply(fdaData: DataFrame,
            targetDimensionColId: String,
            targetDimensionStatsColdId: String)
           (implicit context: ETLSessionContext) = {

    // Define the output
    val outputCols = Set(
      "safetyreportid",
      "chembl_id",
      "reaction_reactionmeddrapt",
      "uniq_report_ids_by_reaction",
      "uniq_report_ids_by_drug",
      "uniq_report_ids",
      targetDimensionColId
    )
    // Define the partition windows
    val wAdverses = Window.partitionBy(col("reaction_reactionmeddrapt"))
    val wTargetDimension = Window.partitionBy(col(targetDimensionColId))
    val wAdverseTargetDimensionComb =
      Window.partitionBy(col(targetDimensionColId), col("reaction_reactionmeddrapt"))
    // and we will need this processed data later on
    val groupedDf = fdaData
      .withColumn("uniq_report_ids_by_reaction", // how many reports mention that reaction
        approx_count_distinct(col("safetyreportid")).over(wAdverses))
      .withColumn(targetDimensionStatsColdId, // how many reports mention that drug
        approx_count_distinct(col("safetyreportid")).over(wTargetDimension))
      .withColumn("uniq_report_ids", // how many mentions of drug-reaction pair
        approx_count_distinct(col("safetyreportid")).over(wAdverseTargetDimensionComb))
      .selectExpr(
        outputCols.toList:_*
      )
    groupedDf
  }
}
