package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.openfda.utils.MathUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, first, lit, udf}

object TargetMonteCarloSampling {
  /**
    * This method runs the Montecarlo sampling on the given input dataset, that counts with Montecarlo input parameters
    * @param inputDf dataset with Montecarlo input parameters
    * @param percentile percentile
    * @param permutations number of permutations
    * @param contextapplication context
    * @return the filtered input dataset according to the Montecarlo calculations
    */
  def apply(inputDf: DataFrame, percentile: Double = 0.99, permutations: Int = 100)(
    implicit context: ETLSessionContext): DataFrame = {

    import context.sparkSession.implicits._
    // Register function with Spark
    val udfCriticalValues: (Int, Int, Seq[Long], Int, Double) => Double =
      MathUtils.calculateCriticalValues
    val udfProbVector = udf(udfCriticalValues)

    // calculate critical values using UDF
    val critValTarget = inputDf
      .withColumn("uniq_reports_total", $"A" + $"B" + $"C" + $"D")
      .withColumn("uniq_report_ids", $"A")
      .groupBy($"targetId")
      .agg(
        first($"uniq_reports_total").as("uniq_reports_total"),
        // This part is different from the original Montecarlo, as it is about pairs (reaction, targetId), and it's
        // probably different depending on the reaction when grouping by target ID
        collect_list($"uniq_report_ids").as("uniq_reports_combined"),
        collect_list($"uniq_report_ids_by_reaction").as("n_i"),
        first($"uniq_report_ids_by_target").as("uniq_report_ids_by_target"),
      )
      // critVal_drug is created using the MonteCarlo method to use a binomial distribution
      // for that particular target
      .withColumn("critVal_target",
        udfProbVector(lit(permutations),
          $"uniq_report_ids_by_target",
          $"n_i",
          $"uniq_reports_total",
          lit(percentile)))
      .select("targetId", "critVal_target")

    val exprs = List(
      "chembl_id",
      "targetId",
      "reaction_reactionmeddrapt as event",
      "A as count",
      "llr",
      "critVal_target as critval",
      "meddraCode"
    )

    val filteredDF = inputDf
      .join(critValTarget, Seq("targetId"), "inner")
      .where(($"llr" > $"critVal_target") and
        ($"critVal_target" > 0))
      .selectExpr(exprs: _*)

    filteredDF

  }
}
