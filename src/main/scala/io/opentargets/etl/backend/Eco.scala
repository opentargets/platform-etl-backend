package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.IOResourceConfig

// This is option/step eco in the config file
object Eco extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val dfName = "eco"
    val common = context.configuration.common
    val mappedInputs = Map(
      "eco" -> IOResourceConfig(common.inputs.eco.format, common.inputs.eco.path)
    )
    val inputDataFrame = Helpers.readFrom(mappedInputs)

    val ecoDF = inputDataFrame(dfName)
      .withColumn("id", substring_index(col("code"), "/", -1))

    val outputs = Seq(dfName)
    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputConfs = outputs
      .map(
        name =>
          name -> IOResourceConfig(context.configuration.common.outputFormat,
                                   context.configuration.common.output + s"/$name"))
      .toMap

    val outputDFs = (outputs zip Seq(ecoDF)).toMap
    Helpers.writeTo(outputConfs, outputDFs)
  }
}
