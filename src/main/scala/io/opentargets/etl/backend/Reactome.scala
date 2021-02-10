package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources

// This is option/step reactome in the config file
object Reactome extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    val dfName = "reactome"
    val common = context.configuration.common
    val mappedInputs = Map(
      dfName -> IOResourceConfig(
        common.inputs.reactome.format,
        common.inputs.reactome.path
      )
    )
    val inputDataFrame = IoHelpers.readFrom(mappedInputs)
    val reactomeDF = inputDataFrame(dfName).data

    val outputs = Map(
      dfName -> IOResource(
        reactomeDF,
        IOResourceConfig(
          context.configuration.common.outputFormat,
          context.configuration.common.output + s"/$dfName"
        )
      )
    )

    IoHelpers.writeTo(outputs)
  }
}
