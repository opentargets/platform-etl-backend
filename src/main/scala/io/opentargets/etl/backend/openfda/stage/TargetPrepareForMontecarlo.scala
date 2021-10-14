package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, log}

object TargetPrepareForMontecarlo {

  /**
    * This method will compute the values that Montecarlo needs, i.e. the number of combined reports (A), the number
    *  of reports per dimension (B and C), and the number of the rest of reports for every combination of reaction and
    *  target dimension / context (D).
    * @param fdaData FAERS data with the summary statistics
    * @param context application context
    * @return the given FAERS dataset that includes the summary statistics, extended with the input parameters Montecarlo
    *         needs
    */
  def apply(fdaData: DataFrame)(implicit context: ETLSessionContext) = {
    import context.sparkSession.implicits._

    // total unique report ids
    val uniqReports: Long = fdaData.select("safetyreportid").distinct.count
    val doubleAgg = fdaData
      .drop("safetyreportid")
      .withColumnRenamed("uniq_report_ids", "A")
      .withColumn("C", col("uniq_report_ids_by_target") - col("A"))
      .withColumn("B", col("uniq_report_ids_by_reaction") - col("A"))
      .withColumn("D",
        lit(uniqReports) - col("uniq_report_ids_by_target") - col(
          "uniq_report_ids_by_reaction") + col("A"))
      .withColumn("aterm", $"A" * (log($"A") - log($"A" + $"B")))
      .withColumn("cterm", $"C" * (log($"C") - log($"C" + $"D")))
      .withColumn("acterm", ($"A" + $"C") * (log($"A" + $"C") - log($"A" + $"B" + $"C" + $"D")))
      .withColumn("llr", $"aterm" + $"cterm" - $"acterm")
      .distinct()
      .where($"llr".isNotNull and !$"llr".isNaN)

    doubleAgg
  }
}
