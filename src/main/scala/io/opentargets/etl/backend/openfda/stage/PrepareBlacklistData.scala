package io.opentargets.etl.backend.openfda.stage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, translate, trim}

object PrepareBlacklistData {
  def apply(dfBlacklistData: DataFrame) = {
    val preparedBlacklistData = dfBlacklistData.toDF("reactions")
      .withColumn("reactions", translate(trim(lower(col("reactions"))), "^", "\\'"))
      .orderBy(col("reactions").asc)

    preparedBlacklistData
  }
}
