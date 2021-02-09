package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{IOResource, IOResourceConfig, IOResources}

// This is option/step MousePhenotypes in the config file. JQ file input
object MousePhenotypes extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val dfName = "mousePhenotypes"
    val common = context.configuration.common
    val mappedInputs = Map(
      dfName -> IOResourceConfig(
        common.inputs.mousephenotypes.format,
        common.inputs.mousephenotypes.path
      )
    )
    val inputDataFrame = Helpers.readFrom(mappedInputs)
    val mousePhenotypesDF = inputDataFrame(dfName).data

    val outputs = Map(
      dfName -> IOResource(
        mousePhenotypesDF,
        IOResourceConfig(
          context.configuration.common.outputFormat,
          context.configuration.common.output + s"/$dfName"
        )
      )
    )
    Helpers.writeTo(outputs)
  }
}
