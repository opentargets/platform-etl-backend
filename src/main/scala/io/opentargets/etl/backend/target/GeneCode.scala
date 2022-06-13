package io.opentargets.etl.backend.target
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, regexp_extract}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * GeneCode is used to extract the canonical transcripts for each Ensembl gene. This information is not presently
  * available in the Ensembl input file, but should be added in Sept 2022.
  */
object GeneCode extends LazyLogging {

  def apply(geneCodeDf: DataFrame)(
      implicit sparkSession: SparkSession): Dataset[CanonicalTranscript] = {
    import sparkSession.implicits._
    logger.info("Extracting canonical transcripts from gene code data.")

    geneCodeDf
      .filter(col("_c2") === "transcript" && col("_c8").contains("Ensembl_canonical"))
      .select(
        regexp_extract(col("_c8"), "gene_id=(.*?);", 1) as "id",
        regexp_extract(col("_c8"), "transcript_id=(.*?);", 1) as "canonicalTranscript"
      )
      .select(
        // Drop the final digit from the ensembl ids: ENSG00000283572.3 -> ENSG00000283572
        regexp_extract(col("id"), "(.*?)\\.", 1) as "id",
        regexp_extract(col("canonicalTranscript"), "(.*?)\\.", 1) as "canonicalTranscript"
      )
      .distinct
      .as[CanonicalTranscript]
  }

}

case class CanonicalTranscript(id: String, canonicalTranscript: String)
