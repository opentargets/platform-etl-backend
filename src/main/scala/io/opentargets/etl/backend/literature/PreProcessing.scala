package io.opentargets.etl.backend.literature

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract, unix_timestamp}

object PreProcessing {

  def removeDuplicatedInformation(df: DataFrame): DataFrame = df
    .groupBy(
      col("pmcid"),
      col("pmid"),
      col("kind")
    )
    .max("int_timestamp")
    .withColumnRenamed("max(int_timestamp)", "int_timestamp")

  def addGroupingColumns(epmc: DataFrame): DataFrame =
    epmc
      .select(col("*"), input_file_name)
      .select(col("*"), regexp_extract(input_file_name, "Full-text|Abstracts", 0) as "kind")
      .withColumn("int_timestamp", unix_timestamp(col("timestamp")))

  def mergeInformationBackToUniques(uniqueDfRows: DataFrame, fullDF: DataFrame): DataFrame =
    uniqueDfRows.join(fullDF, Seq("pmcid", "pmid", "int_timestamp"))

  def removeDuplicateColumns(df: DataFrame, uniqueDfRows: DataFrame): DataFrame =
    df
      .drop(uniqueDfRows.col("kind"))
      .drop(df.col("input_file_name()"))

  def process(epmc: DataFrame)(implicit ss: SparkSession) = {

    val dfWithType = addGroupingColumns(epmc)

    val uniqueDfRows = removeDuplicatedInformation(dfWithType)

    val mergedDfBack = mergeInformationBackToUniques(uniqueDfRows, dfWithType)

    val cleanedAbstractDf = removeDuplicateColumns(mergedDfBack, uniqueDfRows)

    cleanedAbstractDf

  }

}
