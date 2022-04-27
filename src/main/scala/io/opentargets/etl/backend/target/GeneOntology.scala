package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers._
import org.apache.spark.sql.functions.{
  array_union,
  broadcast,
  coalesce,
  col,
  collect_set,
  element_at,
  explode,
  regexp_extract,
  split,
  typedLit
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object GeneOntology extends LazyLogging {

  /** @param human       gene ontology available from [[ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz EBI's database]]
    * @param rna         (human) gene ontology available from [[ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human_rna.gaf.gz EBI's database]]
    * @param rnaLookup   to map from RNACentral -> Ensembl ids available from [[ftp://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/database_mappings/ensembl.tsv EBI]]
    * @param ecoLookup   to map from goId -> eco ids available from [[ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gpa.gz EBI]]
    * @param humanLookup to map from Uniprot accession to Ensembl Id
    */
  def apply(
      human: DataFrame,
      rna: DataFrame,
      rnaLookup: DataFrame,
      ecoLookup: DataFrame,
      humanLookup: Dataset[Ensembl]
  )(implicit sparkSession: SparkSession): Dataset[GeneOntologyByEnsembl] = {
    import sparkSession.implicits._

    logger.info("Transforming Gene Ontology inputs for Target step.")
    // these are all from UniprotKB
    logger.debug("Transforming human gene ontology.")
    val humanGo = extractRequiredColumnsFromRawDf(human)
      .orderBy("goId")

    // all from RNACentral. sample db object id: URS0000001346_9606
    logger.debug("Transforming rna gene ontology")
    val humanRnaGo = extractRequiredColumnsFromRawDf(rna)
      .withColumn("dbObjectId", element_at(split(col("dbObjectId"), "_", 0), 1))
      .orderBy("goId")

    logger.debug("Creating gene ontology lookup tables")
    val rnaLU = rnaToEnsemblLookupTable(rnaLookup)
    val humanLU = ensemblDfToHumanLookupTable(humanLookup)
    val ecoLU = ecoLookupTable(ecoLookup)

    logger.debug("Linking gene ontologies with Ensembl Ids")
    val rnaWithEnsemblId: Dataset[GeneOntologyByEnsembl] = broadcast(
      humanRnaGo
        .join(rnaLU, humanRnaGo("dbObjectId") === rnaLU("rnaCentralId"))
    )
      .join(ecoLU, Seq("goId", "geneProduct", "evidence"), "left_outer")
      .drop("rnaCentralId")
      .transform(nestGO)

    val humanWithEnsemblId: Dataset[GeneOntologyByEnsembl] = humanGo
      .join(broadcast(humanLU), humanGo("dbObjectId") === humanLU("uniprotId"))
      .join(ecoLU, Seq("goId", "geneProduct", "evidence"), "left_outer")
      .drop("uniprotId")
      .transform(nestGO)

    rnaWithEnsemblId
      .withColumnRenamed("go", "go_i")
      .join(humanWithEnsemblId, Seq("ensemblId"), "outer")
      .select(
        col("ensemblId"),
        array_union(
          coalesce(col("go"), typedLit(Array.empty[GO])),
          coalesce(col("go_i"), typedLit(Array.empty[GO]))
        ).as("go")
      )
      .as[GeneOntologyByEnsembl]

  }

  /** Returns required columns from raw dataframe.
    *
    * All columns are renamed according to the [[http://geneontology.org/docs/go-annotation-file-gaf-format-2.2/ GO specification]].
    */
  private def extractRequiredColumnsFromRawDf(dataFrame: DataFrame): DataFrame = {
    val columnNamesFromSpecification = Seq(
      "database",
      "dbObjectId",
      "dbObjectSymbol",
      "qualifier",
      "GoId",
      "dbReference",
      "evidenceCode",
      "withOrFrom",
      "aspect",
      "dbObjectName",
      "dbObjectSynonym",
      "dbObjectType",
      "taxon",
      "date",
      "assignedBy",
      "annotationExtension",
      "geneProductFormId"
    )
    dataFrame
      .toDF(columnNamesFromSpecification: _*)
      .select(
        col("dbObjectId"),
        col("goId"),
        col("dbReference").as("source"),
        col("evidenceCode").as("evidence"),
        col("aspect"),
        col("dbObjectId").as("geneProduct")
      )
  }

  /** Returns lookup table from RNACentral identifiers to Ensembl Gene Ids.
    *
    * Only Ensembl Genes starting with ENSG are included.
    */
  private def rnaToEnsemblLookupTable(dataFrame: DataFrame): DataFrame = {
    val colNames =
      Seq("rnaCentralId", "database", "externalId", "ncbiTaxonId", "rnaType", "ensemblId")

    dataFrame
      .toDF(colNames: _*)
      .select("rnaCentralId", "ensemblId")
      .filter(col("ensemblId").startsWith("ENSG0"))
      .withColumn("ensemblId", regexp_extract(col("ensemblId"), "ENSG[0-9]+", 0))
  }

  /** @return dataframe with columns 'ensemblId', 'uniprotId'
    */
  private def ensemblDfToHumanLookupTable(
      dataset: Dataset[Ensembl]
  )(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    dataset
      .filter(ensg => ensg.proteinIds != null && ensg.proteinIds.nonEmpty)
      .map(row => {
        val ensembId = row.id
        val accessions = row.proteinIds
          .getOrElse(Array.empty)
          .withFilter(_.source.contains("uniprot"))
          .map(pids => pids.id) :+ row.approvedSymbol
        (ensembId, accessions.distinct)
      })
      .toDF("ensemblId", "uniprotId")
      .withColumn("uniprotId", explode(col("uniprotId")))
  }

  /** Returns dataframe with columns [ensemblId, go] */
  private def nestGO(
      dataFrame: DataFrame
  )(implicit sparkSession: SparkSession): Dataset[GeneOntologyByEnsembl] = {
    import sparkSession.implicits._
    dataFrame
      .drop("dbObjectId")
      .withColumnRenamed("goId", "id")
      .transform(nest(_, List("id", "source", "evidence", "aspect", "geneProduct", "ecoId"), "go"))
      .groupBy("ensemblId")
      .agg(collect_set(col("go")).as("go"))
      .withColumn("go", coalesce(col("go"), typedLit(Seq.empty)))
      .as[GeneOntologyByEnsembl]
  }

  /** Returns a dataframe with columns [goId, ecoId] */
  private def ecoLookupTable(df: DataFrame): DataFrame = {
    df.select(
      col("_c3") as "goId",
      col("_c1") as "geneProduct",
      col("_c5") as "ecoId",
      split(col("_c11"), "=")(1) as "evidence"
    ).orderBy(col("goId"))
      .distinct
      .persist(StorageLevel.MEMORY_AND_DISK)
  }
}

case class GeneOntologyByEnsembl(ensemblId: String, go: Array[GO])

case class GO(
    id: String,
    source: String,
    evidence: String,
    aspect: String,
    geneProduct: String,
    ecoId: String
)
