package io.opentargets.etl.backend.variant

import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{readFrom, writeTo}
import org.apache.spark.sql.DataFrame

object VariantIndex {

  private def readInputs()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    val config = context.configuration.variantIndex

    val mappedInputs = Map("variants" -> config.input)

    readFrom(mappedInputs)
  }

  private def writeOutput(variants: DataFrame)(implicit context: ETLSessionContext) = {
    val config = context.configuration.variantIndex

    val mappedOutputs = Map("variantIndex" -> IOResource(variants, config.output))

    writeTo(mappedOutputs)
  }

  def apply()(implicit context: ETLSessionContext): Unit = {


    val inputs = readInputs

    val variantIndexDF = inputs("variants").data

    writeOutput(variantIndexDF)
  }

}
