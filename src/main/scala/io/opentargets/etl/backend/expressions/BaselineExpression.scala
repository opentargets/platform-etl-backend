package io.opentargets.etl.backend.expressions

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BaselineExpression extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val sparkSession: SparkSession = context.sparkSession

    logger.info("Start step baselineExpression")

    val config = context.configuration.baselineExpression

    val mappedInputs = Map(
      "target" -> config.inputs.target,
      "expressions" -> config.inputs.baseline
    )

    val inputDataframes = IoHelpers.readFrom(mappedInputs)
    logger.info("inputs loaded for the step baselineExpression")

    val targetDf = inputDataframes("target").data
    val expressionsDf = inputDataframes("expressions").data

    val targetIdsDf = targetDf.select("id")

    val joindExpressionsDf = expressionsDf
      .join(targetIdsDf, col("ensemblGeneId") === targetDf.col("id"), "outer")

    logger.info("Filtering valid baselineExpression")
    val validExpressionsDf = joindExpressionsDf
      .where(col("id").isNotNull)
      .drop("id")

    val mappedOutputs: IOResources = Map(
      "baselineExpression" -> IOResource(validExpressionsDf, config.outputs.baseline)
    )

    IoHelpers.writeTo(mappedOutputs)

    logger.info("outputs written for the step baselineExpression")

    if (config.printMetrics) {

      val invalidExpressions = joindExpressionsDf
        .where(col("id").isNull)
        .count()

      if (invalidExpressions > 0)
        logger.warn(s"there where ${invalidExpressions} invalid baseline expressions")
      else
        logger.info(s"there where no invalid baseline expressions")

    }
    logger.info("End of the step baselineExpression")
  }

}
