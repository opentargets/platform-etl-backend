package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config

// This is option/step reactome in the config file
object Reactome extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val common = context.configuration.common
    val mappedInputs = Map(
      "reactome" -> Map(
        "format" -> common.inputs.reactome.format,
        "path" -> common.inputs.reactome.path
      )
    )
    val inputDataFrame = SparkHelpers.loader(mappedInputs)
    val reactomeDF = inputDataFrame("reactome")

    SparkHelpers.save(reactomeDF, common.output + "/reactome")

  }
}
