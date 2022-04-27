package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.openfda.utils.MathUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, first, lit, udf}

object MonteCarloSampling extends LazyLogging {

  // To enabling running as part of pipeline
  def apply(
      inputDf: DataFrame,
      targetDimensionColId: String,
      targetDimensionStatsColId: String,
      percentile: Double = 0.99,
      permutations: Int = 100
  )(implicit context: ETLSessionContext): DataFrame = {

    logger.info(s"Run Montecarlo sampling on target dimension '${targetDimensionColId}'")
    import context.sparkSession.implicits._
    // Register function with Spark
    val udfCriticalValues: (Int, Int, Seq[Long], Int, Double) => Double =
      MathUtils.calculateCriticalValues
    val udfProbVector = udf(udfCriticalValues)

    // calculate critical values using UDF
    val critVal = inputDf
      .withColumn("uniq_reports_total", $"A" + $"B" + $"C" + $"D")
      .withColumn("uniq_report_ids", $"A")
      .groupBy(col(targetDimensionColId))
      .agg(
        first($"uniq_reports_total").as("uniq_reports_total"),
        collect_list($"uniq_report_ids").as("uniq_reports_combined"),
        collect_list($"uniq_report_ids_by_reaction").as("n_i"),
        first(col(targetDimensionStatsColId)).as(targetDimensionStatsColId)
      )
      // criticalValue is created using the MonteCarlo method to use a binomial distribution
      // for that particular drug.
      .withColumn(
        "criticalValue",
        udfProbVector(
          lit(permutations),
          col(targetDimensionStatsColId),
          $"n_i",
          $"uniq_reports_total",
          lit(percentile)
        )
      )
      .select(targetDimensionColId, "criticalValue")

    val exprs = List(
      targetDimensionColId,
      "reaction_reactionmeddrapt as event",
      "A as count",
      "llr",
      "criticalValue as critval",
      "meddraCode"
    )

    val filteredDF = inputDf
      .join(critVal, Seq(targetDimensionColId), "inner")
      .where(
        ($"llr" > $"criticalValue") and
          ($"criticalValue" > 0)
      )
      .selectExpr(exprs: _*)

    filteredDF

  }

}
