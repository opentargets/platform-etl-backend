package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, col}

object PrepareSummaryStatistics extends LazyLogging {
  def apply(fdaData: DataFrame, targetDimensionColId: String, targetDimensionStatsColdId: String)(
      implicit context: ETLSessionContext
  ) = {

    logger.info(s"Prepare Summary Statistics for target dimension '${targetDimensionColId}'")
    // Define the output
    val outputCols = List(
      "safetyreportid",
      "reaction_reactionmeddrapt",
      "uniq_report_ids_by_reaction",
      targetDimensionStatsColdId,
      "uniq_report_ids",
      targetDimensionColId
    )

    // set the columns to not repeat code
    val reportIdC = col("safetyreportid")
    val aeC = col("reaction_reactionmeddrapt")

    // Define the partition windows
    val wAdverses = Window.partitionBy(aeC)
    val wTargetDimension = Window.partitionBy(col(targetDimensionColId))
    val wAdverseTargetDimensionComb =
      Window.partitionBy(col(targetDimensionColId), aeC)
    // and we will need this processed data later on
    val groupedDf = fdaData
      .withColumn(
        "uniq_report_ids_by_reaction", // how many reports mention that reaction
        approx_count_distinct(reportIdC).over(wAdverses)
      )
      .withColumn(
        targetDimensionStatsColdId, // how many reports mention that drug
        approx_count_distinct(reportIdC).over(wTargetDimension)
      )
      .withColumn(
        "uniq_report_ids", // how many mentions of drug-reaction pair
        approx_count_distinct(reportIdC).over(wAdverseTargetDimensionComb)
      )
      .selectExpr(outputCols: _*)
    groupedDf
  }
}
