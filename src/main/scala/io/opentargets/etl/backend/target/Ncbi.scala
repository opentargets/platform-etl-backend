package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers._
import io.opentargets.etl.backend.target.TargetUtils.transformArrayToStruct
import org.apache.spark.sql.functions.{col, collect_set, explode, flatten, split, typedLit, filter}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Ncbi(
    id: String,
    synonyms: Array[LabelAndSource],
    symbolSynonyms: Array[LabelAndSource],
    nameSynonyms: Array[LabelAndSource]
)

/** Ncbi data available from ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
  */
object Ncbi extends LazyLogging {
  def apply(df: DataFrame)(implicit sparkSession: SparkSession): Dataset[Ncbi] = {
    import sparkSession.implicits._

    logger.info("Processing Ncbi entrez data")

    val sep = "\\|"
    val ncbiDF = df
      .select(
        split(col("Symbol"), sep).as("sy"),
        split(col("dbXrefs"), sep).as("id"),
        split(col("Synonyms"), sep).as("s"),
        split(col("Other_designations"), sep).as("od")
      )
      .withColumn("id", explode(col("id")))
      .filter(col("id").startsWith("Ensembl"))
      .withColumn("id", split(col("id"), ":"))
      .withColumn("id", explode(col("id")))
      .filter(col("id").startsWith("ENSG"))
      .select(
        col("id"),
        safeArrayUnion(col("s"), col("od"), col("sy")).as("synonyms"),
        safeArrayUnion(col("s"), col("sy")).as("symbolSynonyms"),
        safeArrayUnion(col("od")).as("nameSynonyms")
      )
      .groupBy("id")
      .agg(
        flatten(collect_set("synonyms")).as("synonyms"),
        flatten(collect_set("symbolSynonyms")).as("symbolSynonyms"),
        flatten(collect_set("nameSynonyms")).as("nameSynonyms")
      )

    val transformedNCBI = List("synonyms", "symbolSynonyms", "nameSynonyms")
      .foldLeft(ncbiDF) { (B, name) =>
        B.withColumn(
          name,
          transformArrayToStruct(
            filter(col(name), c => c !== "-"),
            typedLit("NCBI_entrez") :: Nil,
            labelAndSourceSchema
          )
        )
      }

    transformedNCBI.as[Ncbi]
  }
}
