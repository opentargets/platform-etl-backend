package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.{DataFrame, SparkSession}

object MousePhenotype extends Serializable with LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {
    val config = context.configuration.steps.mousePhenotype
    implicit val ss: SparkSession = context.sparkSession
    implicit val target_df: DataFrame = IoHelpers.loadFileToDF(config.input("target"))

    logger.info(s"MousePhenotypes step")
    logger.info(s"MousePhenotypes Reading input data")

    val inputs = config.input
    val inputDataframes = IoHelpers.readFrom(inputs)
    val mousePhenotypesDf = inputDataframes("mouse_phenotypes").data

    logger.info(s"MousePhenotypes Validating data")

    val (valid_targets_df, missing_targets_df) = validate(mousePhenotypesDf, "targetFromSourceId")
    val outputs = Map(
      "succeeded" -> IOResource(valid_targets_df, config.output("succeeded")),
      "failed" -> IOResource(missing_targets_df, config.output("failed"))
    )

    logger.info(s"MousePhenotypes writing output data")
    IoHelpers.writeTo(outputs)
  }

  /** @param df
    *   to validate
    * @param idColumn
    *   column which contains ENSG ids
    * @param targetDf
    *   output of ETL target step
    * @return
    *   tuple of dataframes: left side includes df with rows removed which did not correspond to a
    *   row in {@code targetDf}. Right side are all the rows which were removed from @{code df}.
    */
  def validate(df: DataFrame, idColumn: String)(implicit
      targetDf: DataFrame
  ): (DataFrame, DataFrame) = {

    val cleanedDf = df.join(targetDf, targetDf("id") === df(idColumn), "left_semi")
    val missing = df.join(cleanedDf.select(idColumn), Seq(idColumn), "left_anti")

    (cleanedDf, missing)
  }
}
