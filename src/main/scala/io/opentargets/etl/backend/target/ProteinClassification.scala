package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{
  array,
  arrays_zip,
  col,
  collect_set,
  explode,
  flatten,
  struct,
  typedLit
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class ProteinClassification(accession: String, targetClass: Array[ProteinClassificationEntry])

case class ProteinClassificationEntry(id: Long, label: String, level: String)

object ProteinClassification extends LazyLogging {

  def apply(
      dataFrame: DataFrame
  )(implicit sparkSession: SparkSession): Dataset[ProteinClassification] = {
    import sparkSession.implicits._
    logger.info("Reading protein classifications from ChEMBL.")
    val accessionAndPcDF = dataFrame
      .select(
        explode(
          arrays_zip(col("_metadata.protein_classification"), col("target_components.accession"))
        )
          .as("s")
      )
      .select(col("s.1").as("accession"), col("s.0").as("pc"))
      .select("accession", "pc.*")

    val columns = 1 to 8 map { i =>
      s"l$i"
    }
    val toStruct = (column: String) => {
      struct(
        col("protein_class_id").as("id"),
        col(column).as("label"),
        typedLit(column).as("level")
      )
    }

    val proteinClassificationExpandedDF = columns.foldLeft(accessionAndPcDF)((df, level) => {
      df.withColumn(level, toStruct(level))
    })

    proteinClassificationExpandedDF
      .select(col("accession"), array(columns.head, columns.tail: _*).as("levels"))
      .groupBy("accession")
      .agg(flatten(collect_set("levels")).as("levels"))
      .select(col("accession"), explode(col("levels")).as("l"))
      .select(col("accession"), col("l.*"))
      .filter(col("label").isNotNull)
      .select(col("accession"), struct(col("id"), col("label"), col("level")).as("pc"))
      .groupBy("accession")
      .agg(collect_set(col("pc")).as("targetClass"))
      .as[ProteinClassification]

  }
}
