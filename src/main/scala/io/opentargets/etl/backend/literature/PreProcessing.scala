package io.opentargets.etl.backend.literature

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, input_file_name, lit, regexp_extract, unix_timestamp}

object PreProcessing {

  def removeDuplicatedInformation(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val groupedDF = df
      .groupBy(
        col("pmcid"),
        col("pmid"),
        col("kind")
      )
      .max("int_timestamp")

    groupedDF
  }

  def addGroupingColumns(epmc: DataFrame): DataFrame = {
    epmc
      .select(col("*"), input_file_name)
      .select(col("*"), regexp_extract(input_file_name, "full-text|abstracts", 0) as "kind")
      .withColumn("int_timestamp", unix_timestamp(col("timestamp")))
  }

  def mergeInformationBackToUniques(uniqueDfRows: DataFrame, fullDF: DataFrame): DataFrame = {
    uniqueDfRows.join(fullDF,
      uniqueDfRows("pmcid") === fullDF("pmcid")
        && uniqueDfRows("pmid") === fullDF("pmid")
        && uniqueDfRows("max(int_timestamp)") === fullDF("int_timestamp")
    )
  }

  def removeDuplicateColumns(df: DataFrame, uniqueDfRows: DataFrame): DataFrame = {
    df
      .drop(uniqueDfRows.col("pmcid"))
      .drop(uniqueDfRows.col("pmid"))
      .drop(uniqueDfRows.col("kind"))
      .drop(df.col("max(int_timestamp)"))
      .drop(df.col("int_timestamp"))
      .drop(df.col("input_file_name()"))
  }

  def process(epmc: DataFrame)(implicit ss: SparkSession) = {

    val dfWithType = addGroupingColumns(epmc)

    val uniqueDfRows = removeDuplicatedInformation(dfWithType)

    val mergedDfBack = mergeInformationBackToUniques(uniqueDfRows, dfWithType)

    val cleanedAbstractDf = removeDuplicateColumns(mergedDfBack, uniqueDfRows)

    cleanedAbstractDf

  }

}
