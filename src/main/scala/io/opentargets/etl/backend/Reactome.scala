package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig

// This is option/step reactome in the config file
object Reactome extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val dfName = "reactome"
    val common = context.configuration.common
    val mappedInputs = Map(
      dfName -> IOResourceConfig(
        common.inputs.reactome.format,
        common.inputs.reactome.path
      )
    )
    val inputDataFrame = SparkHelpers.readFrom(mappedInputs)
    val reactomeDF = inputDataFrame(dfName)

    val outputs = Seq(dfName)

    // TODO THIS NEEDS MORE REFACTORING WORK AS IT CAN BE SIMPLIFIED
    val outputConfs = outputs
      .map(
        name =>
          name -> IOResourceConfig(context.configuration.common.outputFormat,
                                   context.configuration.common.output + s"/$name"))
      .toMap

    val outputDFs = (outputs zip Seq(reactomeDF)).toMap
    SparkHelpers.writeTo(outputConfs, outputDFs)
  }
}
