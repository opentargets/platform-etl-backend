package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{nest, safeArrayUnion}
import io.opentargets.etl.backend.target.TargetUtils.transformColumnToIdAndSourceStruct
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

case class Ensembl(id: String,
                   biotype: String,
                   approvedName: String,
                   alternativeGenes: Array[String],
                   genomicLocation: GenomicLocation,
                   approvedSymbol: String,
                   proteinIds: Array[IdAndSource],
                   transcriptIds: Array[String],
                   signalP: Array[IdAndSource])

case class IdAndSource(id: String, source: String)

case class GenomicLocation(chromosome: String, start: Long, end: Long, strand: Integer)

object Ensembl extends LazyLogging {

  val includeChromosomes: List[String] = (1 to 22).toList.map(_.toString) ::: List("X", "Y", "MT")

  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Ensembl] = {
    logger.info("Transforming Ensembl inputs.")
    import ss.implicits._
    val ensemblDF: DataFrame = df
      .filter(col("id").startsWith("ENSG"))
      .filter(
        col("seq_region_name").isin(includeChromosomes: _*) || col("Uniprot/SWISSPROT").isNotNull)
      .select(
        trim(col("id")).as("id"),
        regexp_replace(col("biotype"), "(?i)tec", "").as("biotype"),
        col("description"),
        col("end").cast(LongType),
        col("start").cast(LongType),
        col("strand").cast(IntegerType),
        col("seq_region_name").as("chromosome"), // chromosome
        col("name").as("approvedSymbol"),
        col("transcripts.id") as "transcriptIds",
        col("signalP"),
        col("Uniprot/SPTREMBL").as("uniprot_trembl"),
        col("Uniprot/SWISSPROT").as("uniprot_swissprot"),
        flatten(col("transcripts.translations")).as("translations")
      )
      .orderBy(col("id").asc)
      .persist()
      .transform(nest(_, List("chromosome", "start", "end", "strand"), "genomicLocation"))
      .transform(descriptionToApprovedName)
      .transform(refactorProteinId)
      .transform(refactorSignalP)
      .transform(selectBestNonReferenceGene)

    ensemblDF.as[Ensembl]
  }

  /** Returns dataframe with only one non-encoding gene per approvedSymbol. The other gene ids pointing to the same
    * approvedSymbol are listed in `alternativeGenes`.
    *
    * The exception is that when more than one gene with the same gene ID points to different chromosomes, each are
    * retained: eg.U2, U4, Y_RNA each have multiple EnsemblIds on different chromosomes.
    *
    * In cases where there is a gene on the canonical chromosome with the same approvedSymbol as the non-encoding gene,
    * all non-encoding genes will be listed as alternative genes to the gene on the canonical chromosome.
    *
    * In cases where there are multiple gene ids on non-canonical chromosomes, the longest will be chosen, with the
    * longest being calculated as gene_end - gene_start. If there are multiple gene ids with the same length one will
    * be chosen at random.
    *
    * All alternative gene ids which are reviewed are not included in the index will be included in the alternative id
    * field.
    */
  def selectBestNonReferenceGene(dataFrame: DataFrame): DataFrame = {

    val approvedSymbolWithMoreThanOneGeneId = dataFrame
      .select("id", "approvedSymbol", "genomicLocation.*")
      .groupBy(col("approvedSymbol"))
      .agg(
        count(lit(1)) as "count",
        collect_set(
          struct(
            // we need the -1 to use array_sort later which takes the 'natural ordering'. This ensure the longest is first.
            lit(-1) * (col("end") - col("start")) as "length",
            col("chromosome"),
            col("id")
          )
        ) as "agTemp"
      )
      .filter(col("count") > 1)

    // in cases where there is a gene id on a canonical chromosome we want to use that as the 'reference' id.
    val geneOptionsContainCanonicalChromosome = approvedSymbolWithMoreThanOneGeneId.withColumn(
      "isCanonical",
      exists(col("agTemp.chromosome"), (col: Column) => col.isInCollection(includeChromosomes)))

    /*
    Use this df to add the alt genes to the ensemblId with an approved symbol on a canonical chromosome, and then remove
    the altGenes from the main index.
     */
    val altGenesOnCanonicalId = geneOptionsContainCanonicalChromosome
      .filter(col("isCanonical"))
      .withColumn("canonicalId", filter(col("agTemp"), (col: Column) => {
        col.getField("chromosome").isInCollection(includeChromosomes)
      }))
      .withColumn("altGenes", filter(col("agTemp"), (col: Column) => {
        !col.getField("chromosome").isInCollection(includeChromosomes)
      }))
      .filter(size(col("canonicalId")) === 1) // filter because we don't want to aggregate AG on symbols on several canonical chromosomes.
      .select(
        expr("canonicalId.id[0]") as "id",
        col("altGenes.id") as "altGenes"
      )

    /*
    Use this df to add the alt genes to the ensemblIds with no approved symbol on canonical chromosome,
    then remove the alt genes from main index
     */
    val altGenesOnNonCanonicalId = geneOptionsContainCanonicalChromosome
      .filter(!col("isCanonical"))
      .select(
        col("approvedSymbol"),
        array_sort(col("agTemp")) as "ag"
      )
      .select(col("approvedSymbol"),
              col("ag.id").getItem(0) as "id",
              col("ag.id") as "alternativeGenes")
      .select(col("id"), array_remove(col("alternativeGenes"), col("id")) as "alternativeGenes")

    val IdsToRemove = altGenesOnCanonicalId
      .join(altGenesOnNonCanonicalId,
            altGenesOnCanonicalId("id") === altGenesOnNonCanonicalId("id"),
            "full_outer")
      .select(
        flatten(array(coalesce(col("altGenes"), array()),
                      coalesce(col("alternativeGenes"), array()))) as "genes"
      )
      .select(explode(col("genes")) as "geneToRemove")

    dataFrame
      .join(altGenesOnCanonicalId.orderBy(col("id")), Seq("id"), "left_outer")
      .join(altGenesOnNonCanonicalId.orderBy(col("id")), Seq("id"), "left_outer")
      .join(IdsToRemove.orderBy(col("geneToRemove")),
            col("id") === col("geneToRemove"),
            "left_anti")
      .withColumn("alternativeGenes", coalesce(col("alternativeGenes"), col("altGenes")))
      .drop("altGenes")
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
      .withColumn("approvedName", regexp_replace(col("approvedName"), "(?i)tec", ""))
      .drop(d)
  }

  private def refactorSignalP(dataframe: DataFrame): DataFrame = {
    val signalP =
      dataframe.transform(transformColumnToIdAndSourceStruct("id", "signalP", "signalP"))
    dataframe.drop("signalP").join(signalP, Seq("id"), "left_outer")
  }

}
