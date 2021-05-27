package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import org.apache.spark.sql.SparkSession

// This is option/step expression in the config file
object Expression extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val dfName = "baselineExpression"
    val common = context.configuration.common
    val mappedInputs = Map(
      s"$dfName" -> IOResourceConfig(
        common.inputs.expression.format,
        common.inputs.expression.path
      )
    )
    val inputDataFrame = IoHelpers.readFrom(mappedInputs)

    val expressionDF = inputDataFrame(dfName).data.withColumnRenamed("gene", "id")

    val outputs = Map(
      dfName -> IOResource(expressionDF,
                           IOResourceConfig(
                             context.configuration.common.outputFormat,
                             context.configuration.common.output + s"/$dfName"
                           ))
    )

    IoHelpers.writeTo(outputs)
  }
}
