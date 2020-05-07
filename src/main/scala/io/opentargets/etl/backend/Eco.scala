package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config

// This is option/step eco in the config file
object Eco extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val common = context.configuration.common
    val mappedInputs = Map(
      "eco" -> Map("format" -> common.inputs.eco.format, "path" -> common.inputs.eco.path)
    )
    val inputDataFrame = SparkHelpers.loader(mappedInputs)
    val ecoDF = inputDataFrame("eco")
      .withColumn("id", substring_index(col("code"), "/", -1))

    SparkHelpers.save(ecoDF, common.output + "/eco")

  }
}
