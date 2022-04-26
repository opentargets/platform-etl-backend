package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{array_contains, col, collect_set, struct, typedLit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Hallmarks(attributes: Array[Hallmark], cancerHallmarks: Array[CancerHallmark])

case class Hallmark(pmid: Long, attribute_name: String, description: String)

case class CancerHallmark(pmid: Long, description: String, label: String, impact: String)

case class HallmarksWithId(approvedSymbol: String, hallmarks: Hallmarks)

object Hallmarks extends LazyLogging {

  def apply(rawDf: DataFrame)(implicit sparkSession: SparkSession): Dataset[HallmarksWithId] = {
    import sparkSession.implicits._

    logger.info("Transforming hallmarks inputs...")

    //val df = rawDf.columns.foldLeft(rawDf)((d, n) => d.withColumnRenamed(n, n.toLowerCase))
    val df = rawDf.select(
      col("GENE_SYMBOL") as "gene_symbol",
      //col("CELL_TYPE") as "cell_type",
      col("PUBMED_PMID").cast(LongType) as "pmid",
      col("HALLMARK") as "hallmark",
      col("IMPACT") as "impact",
      col("DESCRIPTION") as "description"
      //col("CELL_LINE") as "cell_line"
    )
    val cancerHallmarks = Seq(
      "proliferative signalling",
      "invasion and metastasis",
      "suppression of growth",
      "angiogenesis",
      "change of cellular energetics",
      "genome instability and mutations",
      "escaping programmed cell death",
      "tumour promoting inflammation",
      "cell replicative immortality",
      "escaping immune response to cancer"
    )
    // add column 'is_cancer_hallmark' so we can split into cancer and non-cancer hallmarks.
    val isCancerDF = df
      .withColumn("cancer_hallmarks", typedLit(cancerHallmarks))
      .withColumn("is_cancer_hallmark", array_contains(col("cancer_hallmarks"), col("hallmark")))
      .drop("cancer_hallmarks")

    val cancerHallmarkDF = isCancerDF
      .filter(col("is_cancer_hallmark"))
      .select(
        col("gene_symbol"),
        struct(
          col("pmid"),
          col("description"),
          col("impact"),
          col("hallmark") as "label"
        ).as("cancerHallmarks")
      )
      .groupBy("gene_symbol")
      .agg(collect_set("cancerHallmarks") as "cancerHallmarks")

    val hallmarksDF = isCancerDF
      .filter(!col("is_cancer_hallmark"))
      .select(
        col("gene_symbol"),
        struct(
          col("pmid"),
          col("description"),
          col("hallmark") as "attribute_name"
        ).as("attributes")
      )
      .groupBy("gene_symbol")
      .agg(collect_set("attributes") as "attributes")

    isCancerDF
      .select("gene_symbol")
      .distinct
      .join(cancerHallmarkDF, Seq("gene_symbol"), "left_outer")
      .join(hallmarksDF, Seq("gene_symbol"), "left_outer")
      .select(
        col("gene_symbol") as "approvedSymbol",
        struct(
          col("attributes"),
          col("cancerHallmarks")
        ).as("hallmarks")
      )
      .as[HallmarksWithId]
  }
}
