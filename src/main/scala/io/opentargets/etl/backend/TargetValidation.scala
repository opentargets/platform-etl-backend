package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.TargetValidation.validate
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import scala.util.Random

object TargetValidation extends Serializable with LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    implicit val target_df: DataFrame =
      IoHelpers.loadFileToDF(context.configuration.targetValidation.target)
    val rs: Map[String, IOResource] = {
      context.configuration.targetValidation.inputs flatMap { it =>
        logger.info(s"Target Validation -- checking ${it.name}")
        val df = IoHelpers.loadFileToDF(it.data)
        val (valid_targets_df, missing_targets_df) = validate(df, it.idColumn)
        Seq(
          s"${it.name}-failed" -> IOResource(
            missing_targets_df,
            context.configuration.targetValidation.output.failed
              .copy(
                path = s"${context.configuration.targetValidation.output.failed.path}/${it.name}"
              )
          ),
          s"${it.name}-succeeded" -> IOResource(
            valid_targets_df,
            context.configuration.targetValidation.output.succeeded
              .copy(path =
                s"${context.configuration.targetValidation.output.succeeded.path}/${it.name}"
              )
          )
        )
      }
    } toMap

    IoHelpers.writeTo(rs)
  }

  /** @param df       to validate
    * @param idColumn column which contains ENSG ids
    * @param targetDf output of ETL target step
    * @return tuple of dataframes: left side includes df with rows removed which did not correspond to a row in
    *         {@code targetDf}. Right side are all the rows which were removed from @{code df}.
    */
  def validate(df: DataFrame, idColumn: String)(implicit
      targetDf: DataFrame
  ): (DataFrame, DataFrame) = {

    val cleanedDf = df
      .join(targetDf, targetDf("id") === df(idColumn), "left_semi")

    val missing = df.join(cleanedDf.select(idColumn), Seq(idColumn), "left_anti")

    (cleanedDf, missing)
  }
}
