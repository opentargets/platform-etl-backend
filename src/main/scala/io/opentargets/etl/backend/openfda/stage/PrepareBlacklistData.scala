package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, translate, trim}

object PrepareBlacklistData extends LazyLogging {
  def apply(dfBlacklistData: DataFrame) = {
    logger.info("Get blacklist of FAERS events")
    val preparedBlacklistData = dfBlacklistData
      .toDF("reactions")
      .withColumn("reactions", translate(trim(lower(col("reactions"))), "^", "\\'"))
      .orderBy(col("reactions").asc)

    preparedBlacklistData
  }
}
