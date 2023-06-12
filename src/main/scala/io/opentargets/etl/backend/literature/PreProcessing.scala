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
      .withColumn("int_timestamp", unix_timestamp(col("timestamp")))

  def mergeInformationBackToUniques(uniqueDfRows: DataFrame, fullDF: DataFrame): DataFrame =
    uniqueDfRows.join(fullDF,
      uniqueDfRows.col("pmcid") <=> fullDF.col("pmcid")
                        && uniqueDfRows.col("pmid") <=> fullDF.col("pmid")
                        && uniqueDfRows.col("int_timestamp") <=> fullDF.col("int_timestamp")
    )

  def removeDuplicateColumns(df: DataFrame, uniqueDfRows: DataFrame): DataFrame =
    df
      .drop(uniqueDfRows.col("kind"))
      .drop(uniqueDfRows.col("pmcid"))
      .drop(uniqueDfRows.col("pmid"))
      .drop(uniqueDfRows.col("int_timestamp"))

  def process(epmc: DataFrame)(implicit ss: SparkSession) = {

    val dfWithType = addGroupingColumns(epmc)

    val uniqueDfRows = removeDuplicatedInformation(dfWithType)

    val mergedDfBack = mergeInformationBackToUniques(uniqueDfRows, dfWithType)

    val cleanedAbstractDf = removeDuplicateColumns(mergedDfBack, uniqueDfRows)

    cleanedAbstractDf

  }

}
