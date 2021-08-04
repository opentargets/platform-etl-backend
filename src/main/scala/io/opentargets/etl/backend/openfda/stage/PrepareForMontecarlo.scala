package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, log}

/*
    Project     : io-opentargets-etl-backend
    Timestamp   : 2021-08-04T14:03
    Author      : Manuel Bernal Llinares <mbdebian@gmail.com>
*/

object PrepareForMontecarlo {
  def apply(fdaData: DataFrame)(implicit context: ETLSessionContext) = {
    import context.sparkSession.implicits._

    // total unique report ids
    val uniqReports: Long = fdaData.select("safetyreportid").distinct.count
    val doubleAgg = fdaData
      .drop("safetyreportid")
      .withColumnRenamed("uniq_report_ids", "A")
      .withColumn("C", col("uniq_report_ids_by_drug") - col("A"))
      .withColumn("B", col("uniq_report_ids_by_reaction") - col("A"))
      .withColumn("D",
        lit(uniqReports) - col("uniq_report_ids_by_drug") - col(
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
