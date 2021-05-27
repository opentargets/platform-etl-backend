package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import io.opentargets.etl.backend.spark.Helpers.stripIDFromURI
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources

// This is option/step eco in the config file
object Eco extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val dfName = "eco"
    val common = context.configuration.common
    val mappedInputs = Map(
      "eco" -> IOResourceConfig(common.inputs.eco.format, common.inputs.eco.path)
    )
    val inputDataFrame = IoHelpers.readFrom(mappedInputs)

    val ecoDF = inputDataFrame(dfName).data
      .withColumn("id", stripIDFromURI(col("code")))

    val outputs = Map(
      dfName -> IOResource(ecoDF,
                           IOResourceConfig(context.configuration.common.outputFormat,
                                            context.configuration.common.output + s"/$dfName"))
    )

    IoHelpers.writeTo(outputs)
  }
}
