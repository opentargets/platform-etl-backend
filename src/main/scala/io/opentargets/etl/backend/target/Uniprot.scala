package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers._
import io.opentargets.etl.backend.target.TargetUtils.transformArrayToStruct
import io.opentargets.etl.preprocess.uniprot.UniprotEntry
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  * @param uniprotId            current accession number
  * @param synonyms             uniprot recommended and alternative names
  * @param functionDescriptions from uniprot comments
  * @param proteinIds           old accession numbers
  * @param subcellularLocations from uniprot comments
  * @param dbXrefs              references to other libraries.
  */
case class Uniprot(
    uniprotId: String,
    synonyms: Seq[LabelAndSource],
    symbolSynonyms: Seq[LabelAndSource],
    nameSynonyms: Seq[LabelAndSource],
    functionDescriptions: Seq[String],
    proteinIds: Seq[IdAndSource],
    subcellularLocations: Seq[LocationAndSource],
    dbXrefs: Seq[IdAndSource]
)

object Uniprot extends LazyLogging {

  val id = "uniprotId"

  def apply(dfRaw: DataFrame)(implicit ss: SparkSession): Dataset[Uniprot] = {
    logger.info("Processing Uniprot inputs")
    import ss.implicits._
    val uniprotDfWithId = dfRaw
      .as[UniprotEntry]
      .filter(size(col("accessions")) > 0) // null return -1 so remove those too
      .withColumn(id, expr("accessions[0]"))
      .withColumn("nameSynonyms", safeArrayUnion(col("names"), col("synonyms")))
      .withColumn("symbolSynonyms", safeArrayUnion(col("symbolSynonyms")))
      .withColumn("synonyms", safeArrayUnion(col("nameSynonyms"), col("symbolSynonyms")))
      .withColumnRenamed("functions", "functionDescriptions")
      .drop("id", "names")
      .transform(handleDbRefs)
      .withColumn("proteinIds",
                  transformArrayToStruct(col("accessions"),
                                         typedLit("uniprot_obsolete") :: Nil,
                                         idAndSourceSchema))
      .withColumn("subcellularLocations",
                  transformArrayToStruct(col("locations"),
                                         typedLit("uniprot") :: Nil,
                                         locationAndSourceSchema))

    val synonyms = List("synonyms", "symbolSynonyms", "nameSynonyms").foldLeft(uniprotDfWithId) {
      (B, name) =>
        B.withColumn(
          name,
          transformArrayToStruct(col(name), typedLit("uniprot") :: Nil, labelAndSourceSchema))
    }

    synonyms.as[Uniprot]
  }

  private def handleDbRefs(dataFrame: DataFrame): DataFrame = {
    val ref = "dbXrefs"
    dataFrame
      .withColumn(
        ref,
        transform(col(ref), c => {
          val sc = split(c, pattern = " ")
          struct(element_at(sc, 2) :: element_at(sc, 1) :: Nil: _*)
        }).cast(ArrayType(idAndSourceSchema))
      )
  }
}
