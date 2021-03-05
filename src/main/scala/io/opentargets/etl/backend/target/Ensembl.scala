package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{nest, safeArrayUnion}
import io.opentargets.etl.backend.target.TargetUtils.transformColumnToIdAndSourceStruct
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Ensembl(id: String,
                   biotype: String,
                   approvedName: String,
                   alternativeGenes: Array[String],
                   genomicLocation: GenomicLocation,
                   approvedSymbol: String,
                   proteinIds: Array[IdAndSource],
                   transcriptIds: Array[IdAndSource],
                   signalP: Array[IdAndSource])

case class IdAndSource(id: String, source: String)

case class GenomicLocation(chromosome: String, start: Long, end: Long, strand: Integer)

object Ensembl extends LazyLogging {

  val includeChromosomes: List[String] = (1 to 22).toList.map(_.toString) ::: List("X", "Y", "MT")

  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Ensembl] = {
    logger.info("Transforming Ensembl inputs.")
    import ss.implicits._
    val ensemblDS: Dataset[Ensembl] = df
      .filter(col("id").startsWith("ENSG"))
      .filter(
        col("seq_region_name").isin(includeChromosomes: _*) || col("Uniprot/SWISSPROT").isNotNull)
      .select(
        trim(col("id")).as("id"),
        col("biotype"),
        col("description"),
        col("end").cast(LongType),
        col("start").cast(LongType),
        col("strand").cast(IntegerType),
        col("seq_region_name").as("chromosome"), // chromosome
        col("name").as("approvedSymbol"),
        col("transcripts"),
        col("signalP"),
        col("Uniprot/SPTREMBL").as("uniprot_trembl"),
        col("Uniprot/SWISSPROT").as("uniprot_swissprot"),
        flatten(col("transcripts.translations")).as("translations")
      )
      .withColumn("end", col("end").cast(IntegerType))
      .transform(nest(_, List("chromosome", "start", "end", "strand"), "genomicLocation"))
      .transform(descriptionToApprovedName)
      .transform(refactorProteinId)
      .transform(refactorTranscriptId)
      .transform(refactorSignalP)
      .transform(selectBestNonReferenceGene)
      .as[Ensembl]

    ensemblDS
  }

  /** Returns dataframe with only one non-encoding gene per approvedSymbol. The other gene ids pointing to the same
    * approvedSymbol are listed in `alternativeGenes`
    *
    * In cases where there are multiple gene ids, the longest will be chosen, with the longest being calculated as
    * gene_end - gene_start. If there are multiple gene ids with the same length one will be chosen at random.
    *
    * All alternative gene ids which are reviewed are not included in the index will be included in the alternative id
    * field.
    * ENSG00000282841
    *
    * @return
    */
  def selectBestNonReferenceGene(dataFrame: DataFrame): DataFrame = {

    // remove genes in standard chromosomes
    val proteinEncodingGenesDF = dataFrame
      .select("id", "genomicLocation.*", "approvedSymbol")
      .filter(!col("chromosome").isin(includeChromosomes: _*))

    // rank by length (per approvedSymbol)
    val proteinEncodingGenesRankedDF = proteinEncodingGenesDF
      .withColumn("length", col("end") - col("start"))
      .withColumn("rank", rank().over(Window.partitionBy("approvedSymbol").orderBy("length")))

    // get best ranked id and symbol
    val bestGuessByLengthDF = proteinEncodingGenesRankedDF
      .filter(col("rank") === 1)
      .dropDuplicates(Seq("approvedSymbol", "rank")) // get first ranked (longest)
      .dropDuplicates("approvedSymbol") // remove duplicates where there was a tie for length
      .select("id", "approvedSymbol")

    // group nth ranked ids
    val symbolsWithAlternativesDF = proteinEncodingGenesRankedDF
      .groupBy("approvedSymbol")
      .agg(collect_set(col("id")).as("alternativeGenes"))

    val nonReferenceAndAlternativeDF = bestGuessByLengthDF
      .join(symbolsWithAlternativesDF, Seq("approvedSymbol"))
      .withColumn("alternativeGenes", array_remove(col("alternativeGenes"), col("id")))
      .drop("approvedSymbol")

    val allNonReferenceIds = proteinEncodingGenesDF.select("id")
    // get list of reference ids we want
    val nonReferenceIdsWeWantDF = nonReferenceAndAlternativeDF.select("id")
    // remove the non-reference ids from the list to be discarded
    val nonReferenceIdsWeDontWantDF =
      allNonReferenceIds.join(nonReferenceIdsWeWantDF, Seq("id"), "leftanti")

    // filter final dataframe.
    dataFrame
      .join(nonReferenceIdsWeDontWantDF, Seq("id"), "leftanti")
      .join(nonReferenceAndAlternativeDF, Seq("id"), "left_outer")

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

  /** Returns dataframe with column 'proteinIds' added and columns, 'translations', 'uniprot_trembl'
    * and 'uniprot_swissprot' removed.
    *
    * 'proteinIds' includes sources:
    *   - uniprot_swissprot
    *   - uniprot_trembl
    *   - ensembl_PRO
    * */
  def refactorProteinId: DataFrame => DataFrame = { df =>
    {
      val ensemblProDF =
        df.transform(
          transformColumnToIdAndSourceStruct("id",
                                             "translations.id",
                                             "ensembl_PRO",
                                             Some("ensembl_PRO")))
      val uniprotSwissDF: DataFrame =
        df.transform(
          transformColumnToIdAndSourceStruct("id", "uniprot_swissprot", "uniprot_swissprot"))
      val uniprotTremblDF: DataFrame =
        df.transform(transformColumnToIdAndSourceStruct("id", "uniprot_trembl", "uniprot_trembl"))
      val proteinIds = ensemblProDF
        .join(uniprotSwissDF, Seq("id"), "outer")
        .join(uniprotTremblDF, Seq("id"), "outer")
        .select(col("id"),
                safeArrayUnion(col("uniprot_swissprot"), col("uniprot_trembl"), col("ensembl_PRO"))
                  .as("proteinIds"))

      df.drop("uniprot_swissprot", "translations", "uniprot_trembl")
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

  private def refactorSignalP(dataframe: DataFrame): DataFrame = {
    val signalP =
      dataframe.transform(transformColumnToIdAndSourceStruct("id", "signalP", "signalP"))
    dataframe.drop("signalP").join(signalP, Seq("id"), "left_outer")
  }

}
