package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{IOResourceConfig, IOResources}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Target extends LazyLogging {
  def compute(context: ETLSessionContext)(implicit ss: SparkSession): DataFrame = {

    val targetConfig = context.configuration.target
    val mappedInputs = Map(
      "hgnc" -> IOResourceConfig(
        targetConfig.input.hgnc.format,
        targetConfig.input.hgnc.path
      )
    )

    val inputDataFrame = Helpers.readFrom(mappedInputs)

    val hgnc: Option[Dataset[Hgnc]] = inputDataFrame.get("hgnc").map( Hgnc(_))

    ???
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val targetDF = compute(context)

    ???
  }
}

