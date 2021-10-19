package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.openfda.utils.MathUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, first, lit, udf}

object MonteCarloSampling {

  // To enabling running as part of pipeline
  def apply(inputDf: DataFrame,
            targetDimensionColId: String,
            targetDimensionStatsColId: String,
            percentile: Double = 0.99,
            permutations: Int = 100)(
      implicit context: ETLSessionContext): DataFrame = {

    import context.sparkSession.implicits._
    // Register function with Spark
    val udfCriticalValues: (Int, Int, Seq[Long], Int, Double) => Double =
      MathUtils.calculateCriticalValues
    val udfProbVector = udf(udfCriticalValues)

    // calculate critical values using UDF
    val critValDrug = inputDf
      .withColumn("uniq_reports_total", $"A" + $"B" + $"C" + $"D")
      .withColumn("uniq_report_ids", $"A")
      .groupBy(col(targetDimensionColId))
      .agg(
        first($"uniq_reports_total").as("uniq_reports_total"),
        collect_list($"uniq_report_ids").as("uniq_reports_combined"),
        collect_list($"uniq_report_ids_by_reaction").as("n_i"),
        first(col(targetDimensionStatsColId)).as(targetDimensionStatsColId),
      )
      // critVal_drug is created using the MonteCarlo method to use a binomial distribution
      // for that particular drug.
      .withColumn("critVal_drug",
                  udfProbVector(lit(permutations),
                                col(targetDimensionStatsColId),
                                $"n_i",
                                $"uniq_reports_total",
                                lit(percentile)))
      .select(targetDimensionColId, "critVal_drug")

    val exprs = Set(
      "chembl_id",
      targetDimensionColId,
      "reaction_reactionmeddrapt as event",
      "A as count",
      "llr",
      "critVal_drug as critval",
      "meddraCode"
    )

    val filteredDF = inputDf
      .join(critValDrug, Seq(targetDimensionColId), "inner")
      .where(($"llr" > $"critVal_drug") and
        ($"critVal_drug" > 0))
      .selectExpr(exprs.toSeq:_*)

    filteredDF

  }

}
