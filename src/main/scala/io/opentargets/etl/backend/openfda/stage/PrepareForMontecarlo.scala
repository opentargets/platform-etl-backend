package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.openfda.stage.PrepareSummaryStatistics.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, log}

object PrepareForMontecarlo extends LazyLogging {
  def apply(fdaData: DataFrame, targetDimensionStatsColId: String)(implicit
      context: ETLSessionContext
  ) = {
    import context.sparkSession.implicits._

    logger.info(
      s"Prepare data for Montecarlo on target dimension stats '${targetDimensionStatsColId}'"
    )
    // total unique report ids
    val uniqReports: Long = fdaData.select("safetyreportid").distinct.count
    val doubleAgg = fdaData
      .drop("safetyreportid")
      .withColumnRenamed("uniq_report_ids", "A")
      .withColumn("C", col(targetDimensionStatsColId) - col("A"))
      .withColumn("B", col("uniq_report_ids_by_reaction") - col("A"))
      .withColumn(
        "D",
        lit(uniqReports) - col(targetDimensionStatsColId) - col(
          "uniq_report_ids_by_reaction"
        ) + col("A")
      )
      .withColumn("aterm", $"A" * (log($"A") - log($"A" + $"B")))
      .withColumn("cterm", $"C" * (log($"C") - log($"C" + $"D")))
      .withColumn("acterm", ($"A" + $"C") * (log($"A" + $"C") - log($"A" + $"B" + $"C" + $"D")))
      .withColumn("llr", $"aterm" + $"cterm" - $"acterm")
      .distinct()
      .where($"llr".isNotNull and !$"llr".isNaN)

    doubleAgg
  }
}
