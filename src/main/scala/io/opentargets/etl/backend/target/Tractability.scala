package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TractabilityWithId(ensemblGeneId: String, tractability: Array[Tractability])

case class Tractability(modality: String, value: Boolean, id: String)

object Tractability extends LazyLogging {

  def apply(df: DataFrame)(implicit sparkSession: SparkSession): Dataset[TractabilityWithId] = {
    import sparkSession.implicits._
    logger.info("Processing tractability data")

    val gid = "ensembl_gene_id"
    // Only include columns that follow the pattern _B\d+_ (e.g SM_B1_Approved drugs)
    val cols = df.columns.filter(_.matches(".*_B\\d+_.*"))
    val tractabilityDF = df.select(gid, cols: _*)
    val dataColumns = tractabilityDF.columns.filter(_ != gid)
    val mapTractabilityColumnToStruct = (columnName: String) => {
      val nameComponents: Array[String] = columnName.split("_")
      struct(
        lit(nameComponents.head).as("modality"), // first part of column name is modality
        lit(nameComponents.last).as("id"), // last part of column name is id
        when(col(columnName) === 1, true).otherwise(false).as("value")
      )
    }
    val columnsAsTractabilityStructDF = dataColumns.foldLeft(tractabilityDF)((df, nxt) =>
      df.withColumn(nxt, mapTractabilityColumnToStruct(nxt))
    )
    // merge all column into a single array
    columnsAsTractabilityStructDF
      .select(
        col(gid).as("ensemblGeneId"),
        array(dataColumns.head, dataColumns.tail: _*).as("tractability")
      )
      .as[TractabilityWithId]

  }
}
