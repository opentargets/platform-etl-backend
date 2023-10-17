package io.opentargets.etl.backend.expressions

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions._

object BaselineExpression extends LazyLogging{

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val sparkSession = context.sparkSession

    val config = context.configuration.baselineExpression

    val mappedInputs = Map(
      "target" -> config.inputs.target,
      "expressions" -> config.inputs.baseline
    )

    val inputDataframes = IoHelpers.readFrom(mappedInputs)

    val targetDf = inputDataframes("target").data
    val expressionsDf = inputDataframes("expressions").data

    val targetIdsDf = targetDf.select("id")

    val joindExpressionsDf = expressionsDf
      .join(targetIdsDf, col("ensemblGeneId") === targetDf.col("id"), "outer")

    val validExpressionsDf = joindExpressionsDf
      .where(col("id").isNotNull)
      .drop("id")

    val invalidExpressionsDf = joindExpressionsDf
      .where(col("id").isNull)
      .drop("id")

    val mappedOutputs: IOResources = Map(
      "baselineExpression" -> IOResource(validExpressionsDf, config.output)
    )

    IoHelpers.writeTo(mappedOutputs)
  }

}
