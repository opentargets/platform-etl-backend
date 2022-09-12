package io.opentargets.etl.backend.target
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.functions.{col, regexp_extract, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** GeneCode is used to extract the canonical transcripts for each Ensembl gene. This information is
  * not presently available in the Ensembl input file, but should be added in Sept 2022.
  */
object GeneCode extends LazyLogging {

  def apply(
      geneCodeDf: DataFrame
  )(implicit sparkSession: SparkSession): Dataset[GeneAndCanonicalTranscript] = {
    import sparkSession.implicits._
    logger.info("Extracting canonical transcripts from gene code data.")

    geneCodeDf
      .filter(col("_c2") === "transcript" && col("_c8").contains("Ensembl_canonical"))
      .select(
        regexp_extract(col("_c8"), "gene_id=(.*?);", 1) as "id",
        regexp_extract(col("_c8"), "transcript_id=(.*?);", 1) as "canonicalTranscript",
        regexp_extract(col("_c0"), "([0-9]{1,2}|X|Y|M)", 1) as "chromosome",
        col("_c3") as "start",
        col("_c4") as "end",
        col("_c6") as "strand"
      )
      .select(
        // Drop the final digit from the ensembl ids: ENSG00000283572.3 -> ENSG00000283572
        regexp_extract(col("id"), "(.*?)\\.", 1) as "gene_id",
        regexp_extract(col("canonicalTranscript"), "(.*?)\\.", 1) as "id",
        when(col("chromosome") === "M", "MT").otherwise(col("chromosome")) as "chromosome",
        col("start") cast LongType,
        col("end") cast LongType,
        col("strand")
      )
      .distinct
      .transform(nest(_, List("id", "chromosome", "start", "end", "strand"), "canonicalTranscript"))
      .withColumnRenamed("gene_id", "id")
      .as[GeneAndCanonicalTranscript]
  }

}

case class CanonicalTranscript(
    id: String,
    chromosome: String,
    start: Long,
    end: Long,
    strand: String
)
case class GeneAndCanonicalTranscript(id: String, canonicalTranscript: CanonicalTranscript)
