package io.opentargets.etl.backend.target

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ProteinClassification {
  type ProteinClassication = (String, String)

  def apply(dataFrame: DataFrame)(implicit sparkSession: SparkSession): Dataset[(String, String)] =
    ???
}
