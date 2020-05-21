package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig

// This is option/step expression in the config file
object Expression extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val dfName = "expression"
    val common = context.configuration.common
    val mappedInputs = Map(
      "expression" -> IOResourceConfig(
        common.inputs.expression.format,
        common.inputs.expression.path
      )
    )
    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)

    val expressionDF = inputDataFrame(dfName).withColumnRenamed("gene", "id")

    val outputs = Seq(dfName)
    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputConfs = outputs
      .map(name =>
        name -> IOResourceConfig(
          context.configuration.common.outputFormat,
          context.configuration.common.output + s"/$name"
        )
      )
      .toMap

    val outputDFs = (outputs zip Seq(expressionDF)).toMap
    SparkHelpers.writeTo(outputConfs, outputDFs)
  }
}
