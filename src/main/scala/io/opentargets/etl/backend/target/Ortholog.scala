package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.safeArrayUnion
import org.apache.spark.sql.functions.{array_contains, col, collect_list, struct, typedLit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** Maps orthologs to ensembl human gene ids */
object Ortholog extends LazyLogging {

  /**
    * @param geneToOrthMap  from gs://open-targets-data-releases/20.11/input/annotation-files/human_all_hcop_sixteen_column-2020-11-10.txt.gz
    * @param homologyDict   Ensembl dictionary of species: ftp://ftp.ensembl.org/pub/release-100/species_EnsemblVertebrates.txt
    * @param codingProteins Ensembl human genes coding protein ftp://ftp.ensembl.org/pub/release-100/tsv/ensembl-compara/homologies/homo_sapiens/Compara.100.protein_default.homologies.tsv.gz
    * @param ncRna          Ensembl non coding RNA ftp://ftp.ensembl.org/pub/release-100/tsv/ensembl-compara/homologies/homo_sapiens/Compara.100.ncrna_default.homologies.tsv.gz
    * @param targetSpecies  List of whitelisted species taken from the configuration file.
    * @return
    */
  def apply(
      geneToOrthMap: DataFrame,
      homologyDict: DataFrame,
      codingProteins: DataFrame,
      ncRna: DataFrame,
      targetSpecies: List[String])(implicit sparkSession: SparkSession): Dataset[LinkedOrtholog] = {
    import sparkSession.implicits._
    logger.info("Processing homologs.")

    val homoDict = homologyDict
      .select(col("#name").as("name"),
              col("species").as("speciesName"),
              col("taxonomy_id"),
              typedLit(targetSpecies.flatMap(_.split("-").headOption)).as("whitelist"))
      .filter(array_contains(col("whitelist"), col("taxonomy_id")))

    val extractGeneCodingData = (df: DataFrame) => {
      df.filter("is_high_confidence = 1")
        .join(homoDict, col("homology_species") === homoDict("speciesName"))
        .select(
          col("taxonomy_id").as("speciesId"),
          col("name").as("speciesName"),
          col("homology_type").as("homologyType"),
          col("homology_gene_stable_id").as("targetGeneId"),
          col("identity").cast(DoubleType).as("queryPercentageIdentity"),
          col("homology_identity")
            .cast(DoubleType)
            .as("targetPercentageIdentity")
        )
    }

    val homoDF = codingProteins.transform(extractGeneCodingData)
    val rnaDF = ncRna.transform(extractGeneCodingData)

    val proteinDf = linkOrthologToHumanGene(homoDF, geneToOrthMap)
    val rnaDf = linkOrthologToHumanGene(rnaDF, geneToOrthMap)
    proteinDf
      .join(rnaDf.withColumnRenamed("homologues", "h"), Seq("humanGeneId"), "outer")
      .select(col("humanGeneId"), safeArrayUnion(col("homologues"), col("h")).as("homologues"))
      .as[LinkedOrtholog]
  }

  private def linkOrthologToHumanGene(orthologs: DataFrame, humanGenes: DataFrame): DataFrame = {
    val id = "humanGeneId"
    val orthologId = "oid"
    val ensemblIdToGeneIdDF = humanGenes.select(
      col("human_ensembl_gene").as(id),
      col("ortholog_species_ensembl_gene").as(orthologId),
      col("ortholog_species_symbol").as("targetGeneSymbol"))
    val mappedEnsemblToOrth = ensemblIdToGeneIdDF
      .join(orthologs, col(orthologId) === col("targetGeneId"))
      .drop(orthologId)

    val cols = mappedEnsemblToOrth.columns.filter(_ != id)
    mappedEnsemblToOrth
      .select(col(id),
              struct(
                cols.head,
                cols.tail: _*
              ).as("homologues"))
      .groupBy(id)
      .agg(collect_list("homologues").as("homologues"))
  }
}

case class LinkedOrtholog(humanGeneId: String, homologues: Array[Ortholog])

case class Ortholog(speciesId: String,
                    speciesName: String,
                    homologyType: String,
                    targetGeneId: String,
                    targetGeneSymbol: String,
                    queryPercentageIdentity: Double,
                    targetPercentageIdentity: Double)
