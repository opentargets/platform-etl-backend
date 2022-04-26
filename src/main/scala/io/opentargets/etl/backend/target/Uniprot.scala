package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers._
import io.opentargets.etl.backend.target.TargetUtils.transformArrayToStruct
import io.opentargets.etl.preprocess.uniprot.UniprotEntry
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** @param uniprotId            current accession number
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

  /** @param dfRaw      uniprot 'raw' data as prepared from flat file using UniprotConverter.scala
    * @param uniprotSsl mapping to subcellular location ontology
    */
  def apply(dfRaw: DataFrame, uniprotSsl: DataFrame)(implicit
      ss: SparkSession
  ): Dataset[Uniprot] = {
    logger.info("Processing Uniprot inputs")
    import ss.implicits._
    val uniprotDfWithId = dfRaw
      .as[UniprotEntry]
      .filter(size(col("accessions")) > 0) // null return -1 so remove those too
      .select(
        expr("accessions[0]") as id,
        safeArrayUnion(col("names"), col("synonyms")) as "nameSynonyms",
        safeArrayUnion(col("symbolSynonyms")) as "symbolSynonyms",
        safeArrayUnion(
          safeArrayUnion(col("names")),
          safeArrayUnion(col("symbolSynonyms"))
        ) as "synonyms",
        col("functions") as "functionDescriptions",
        col("dbXrefs"),
        col("accessions"),
        col("locations")
      )
      .transform(handleDbRefs)
      .transform(mapLocationsToSsl(uniprotSsl))
      .withColumn(
        "proteinIds",
        transformArrayToStruct(
          col("accessions"),
          typedLit("uniprot_obsolete") :: Nil,
          idAndSourceSchema
        )
      )

    val synonyms = List("synonyms", "symbolSynonyms", "nameSynonyms").foldLeft(uniprotDfWithId) {
      (B, name) =>
        B.withColumn(
          name,
          transformArrayToStruct(col(name), typedLit("uniprot") :: Nil, labelAndSourceSchema)
        )
    }

    synonyms.as[Uniprot]
  }

  private def handleDbRefs(dataFrame: DataFrame): DataFrame = {
    val ref = "dbXrefs"
    dataFrame
      .withColumn(
        ref,
        transform(
          col(ref),
          c => {
            val sc = split(c, pattern = " ")
            struct(element_at(sc, 2) :: element_at(sc, 1) :: Nil: _*)
          }
        ).cast(ArrayType(idAndSourceSchema))
      )
  }

  private def mapLocationsToSsl(sslDf: DataFrame)(df: DataFrame): DataFrame = {

    /** Regexes to meet specification for subcellular locations as in opentargets/platform#1710
      */
    // from the start of the sentence take words and spaces until punctuation or end of line
    val firstWordsRegex = "^([\\w\\s]+)"
    // matches various isoform terms "[isoform xyz]: words..." where we want the first words after closing square brackets.
    val isoformsRegex = "(\\[.+\\]:\\s([\\w\\s]+))"
    // take last term in sequence after a comma.
    val lastWordsAfterCommaRegex = ".*,\\s([\\w\\s]+)"

    val subcellOntologyDf = sslDf.select(
      col("Subcellular location ID") as "termSL",
      col("Alias") as "ssl_match",
      col("Category") as "labelSL"
    )

    val uniprotLocationsDf = df.select(
      col(id),
      explode(col("locations")) as "location"
    )

    // prepare for matching using subcellOntologyDf
    val locationsProcessedDf = uniprotLocationsDf
      .select(
        col(id),
        trim(regexp_extract(col("location"), firstWordsRegex, 0)) as "loc1",
        trim(regexp_extract(col("location"), isoformsRegex, 1)) as "iso",
        trim(regexp_extract(col("location"), isoformsRegex, 2)) as "loc2",
        trim(regexp_extract(col("location"), lastWordsAfterCommaRegex, 1)) as "loc3"
      )
      .withColumn(
        "ssl_match",
        when(col("loc1") =!= "", col("loc1"))
          .when(col("loc2") =!= "", col("loc2"))
          .when(col("loc3") =!= "", col("loc3"))
          .otherwise(lit(null))
      )
      .withColumn("location", when(col("iso") =!= "", col("iso")).otherwise(col("ssl_match")))
      .drop("iso", "loc1", "loc2", "loc3")

    val locationsWithSslTerms = locationsProcessedDf
      .join(broadcast(subcellOntologyDf), Seq("ssl_match"), "left_outer")
      .drop("ssl_match")
      .withColumn("source", lit("uniprot"))
      .transform(nest(_: DataFrame, List("location", "source", "termSL", "labelSL"), "locations"))
      .groupBy("uniprotId")
      .agg(collect_list("locations") as "locations")
      .orderBy(id)

    df.drop("locations")
      .orderBy(id)
      .join(locationsWithSslTerms, Seq(id), "left_outer")
      .withColumnRenamed("locations", "subcellularLocations")

  }
}
