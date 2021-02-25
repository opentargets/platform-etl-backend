package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.nest
import io.opentargets.etl.backend.target.TargetUtils.transformColumnToLabelAndSourceStruct
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Ensembl(id: String,
                   biotype: String,
                   approvedName: String,
                   genomicLocation: GenomicLocation,
                   approvedSymbol: String,
                   proteinIds: Array[IdAndSource],
                   transcriptIds: Array[IdAndSource],
                   signalP: Array[IdAndSource])

case class IdAndSource(id: String, source: String)

case class GenomicLocation(chromosome: String, start: Long, end: Long, strand: Integer)

object Ensembl extends LazyLogging {

  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Ensembl] = {
    logger.info("Transforming Ensembl inputs.")
    import ss.implicits._
    val ensembl: Dataset[Ensembl] = df
      .filter(col("id").startsWith("ENSG"))
      .select(
        col("id"),
        col("biotype"),
        col("description"),
        col("end").cast(LongType),
        col("start").cast(LongType),
        col("strand").cast(IntegerType),
        col("seq_region_name").as("chromosome"), // chromosome
        col("name").as("approvedSymbol"),
        col("protein_id"),
        col("transcripts"),
        col("signalP"),
        flatten(col("transcripts.translations")).as("translations")
      )
      .withColumn("end", col("end").cast(IntegerType))
      .transform(nest(_, List("chromosome", "start", "end", "strand"), "genomicLocation"))
      .transform(descriptionToApprovedName)
      .transform(refactorProteinId)
      .transform(refactorTranscriptId)
      .transform(refactorSignalP)
      .as[Ensembl]

    ensembl
  }

  /** Returns dataframe with column 'transcriptIds' added and column 'transcripts' removed. */
  def refactorTranscriptId: DataFrame => DataFrame = { df =>
    {
      df.join(
          df.select(col("id").as("i"), explode(col("transcripts")).as("t"))
            .select(col("i"), col("t.id").as("id"))
            .withColumn("source", typedLit("Ensembl_TRA"))
            .transform(nest(_, List("source", "id"), "transcriptIds"))
            .withColumnRenamed("i", "id")
            .groupBy("id")
            .agg(collect_set(col("transcriptIds")).as("transcriptIds")),
          Seq("id"),
          "left_outer"
        )
        .drop("transcripts")
    }
  }

  /** Returns dataframe with column 'proteinIds' added and columns 'protein_id' and 'translations' removed.
    *
    * 'proteinIds' includes sources:
    *   - Uniprot
    *   - ensembl_PRO
    * */
  def refactorProteinId: DataFrame => DataFrame = { df =>
    {
      def convertToIdAndSource(dataFrame: DataFrame,
                               source: String,
                               sourceIdColumn: String): DataFrame = {
        dataFrame
          .select(col("id").as("i"), explode(col(sourceIdColumn)).as("id"))
          .withColumn("source", typedLit(source))
          .transform(nest(_, List("id", "source"), "pids"))
          .withColumnRenamed("i", "id")
          .groupBy("id")
          .agg(collect_set(col("pids")).as(source))
      }

      val uniprot = convertToIdAndSource(df, "Uniprot", "protein_id")

      val ensemblPro = convertToIdAndSource(df, "ensembl_PRO", "translations.id")

      val proteinIds = uniprot
        .join(ensemblPro, Seq("id"), "outer")
        .select(col("id"), array_union(col("Uniprot"), col("ensembl_PRO")).as("proteinIds"))

      df.drop("protein_id", "translations")
        .join(
          proteinIds,
          Seq("id"),
          "left_outer"
        )
    }
  }

  /** Return approved name from description */
  private def descriptionToApprovedName(dataFrame: DataFrame): DataFrame = {
    val d = "description"
    dataFrame
      .withColumn(d, split(col(d), "\\[")) // remove redundant source information.
      .withColumn("approvedName", element_at(col(d), 1))
      .drop(d)
  }

  private def refactorSignalP(dataFrame: DataFrame): DataFrame = {
    val signalP =
      transformColumnToLabelAndSourceStruct(dataFrame, "id", "signalP", "signalP", Some("id"))
    dataFrame.drop("signalP").join(signalP, Seq("id"), "left_outer")
  }

}
