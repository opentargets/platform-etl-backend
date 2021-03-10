package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.functions.{
  array_union,
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

object GeneOntology extends LazyLogging {

  /**
    * // todo Add ECO codes as described in #1037
    *
    * @param human       gene ontology available from [[ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz EBI's database]]
    * @param rna         (human) gene ontology available from [[ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human_rna.gaf.gz EBI's database]]
    * @param rnaLookup   to map from RNACentral -> Ensembl ids available from [[ftp://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/database_mappings/ensembl.tsv EBI]]
    * @param humanLookup to map from Uniprot accession to Ensembl Id
    */
  def apply(human: DataFrame, rna: DataFrame, rnaLookup: DataFrame, humanLookup: Dataset[Ensembl])(
      implicit sparkSession: SparkSession): Dataset[GeneOntologyByEnsembl] = {
    import sparkSession.implicits._

    logger.info("Transforming Gene Ontology inputs for Target step.")
    // these are all from UniprotKB
    logger.debug("Transforming human gene ontology.")
    val humanGo = extractRequiredColumnsFromRawDf(human)

    // all from RNACentral. sample db object id: URS0000001346_9606
    logger.debug("Transforming rna gene ontology")
    val humanRnaGo = extractRequiredColumnsFromRawDf(rna)
      .withColumn("dbObjectId", element_at(split(col("dbObjectId"), "_", 0), 1))

    logger.debug("Creating gene ontology lookup tables")
    val rnaLU = rnaToEnsemblLookupTable(rnaLookup)
    val humanLU = ensemblDfToHumanLookupTable(humanLookup)

    logger.debug("Linking gene ontologies with Ensembl Ids")
    val rnaWithEnsemblId: Dataset[GeneOntologyByEnsembl] = humanRnaGo
      .join(rnaLU, humanRnaGo("dbObjectId") === rnaLU("rnaCentralId"))
      .drop("rnaCentralId")
      .transform(nestGO)

    val humanWithEnsemblId: Dataset[GeneOntologyByEnsembl] = humanGo
      .join(humanLU, humanGo("dbObjectId") === humanLU("uniprotId"))
      .drop("uniprotId")
      .transform(nestGO)

    rnaWithEnsemblId
      .withColumnRenamed("go", "go_i")
      .join(humanWithEnsemblId, Seq("ensemblId"), "outer")
      .select(col("ensemblId"),
              array_union(coalesce(col("go"), typedLit(Array.empty[GO])),
                          coalesce(col("go_i"), typedLit(Array.empty[GO]))).as("go"))
      .as[GeneOntologyByEnsembl]

  }

  /**
    * Returns required columns from raw dataframe.
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
      .select(col("dbObjectId"),
              col("goId"),
              col("dbReference").as("source"),
              col("evidenceCode").as("evidence"),
              col("aspect"))
  }

  /** Returns lookup table from RNACentral identifiers to Ensembl Gene Ids.
    *
    * Only Ensembl Genes starting with ENSG are included.
    * */
  private def rnaToEnsemblLookupTable(dataFrame: DataFrame): DataFrame = {
    val colNames =
      Seq("rnaCentralId", "database", "externalId", "ncbiTaxonId", "rnaType", "ensemblId")

    dataFrame
      .toDF(colNames: _*)
      .select("rnaCentralId", "ensemblId")
      .filter(col("ensemblId").startsWith("ENSG0"))
      .withColumn("ensemblId", regexp_extract(col("ensemblId"), "ENSG[0-9]+", 0))
  }

  private def ensemblDfToHumanLookupTable(dataset: Dataset[Ensembl])(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    dataset
      .filter(ensg => ensg.proteinIds != null && ensg.proteinIds.nonEmpty)
      .map(row => {
        val ensembId = row.id
        val accessions = row.proteinIds
          .withFilter(_.source == "Uniprot")
          .map(pids => pids.id)
        (ensembId, accessions)
      })
      .toDF("ensemblId", "uniprotId")
      .withColumn("uniprotId", explode(col("uniprotId")))
  }

  private def nestGO(dataFrame: DataFrame)(
      implicit sparkSession: SparkSession): Dataset[GeneOntologyByEnsembl] = {
    import sparkSession.implicits._
    dataFrame
      .drop("dbObjectId")
      .withColumnRenamed("goId", "id")
      .transform(nest(_, List("id", "source", "evidence", "aspect"), "go"))
      .groupBy("ensemblId")
      .agg(collect_set(col("go")).as("go"))
      .withColumn("go", coalesce(col("go"), typedLit(Seq.empty)))
      .as[GeneOntologyByEnsembl]
  }
}

case class GeneOntologyByEnsembl(ensemblId: String, go: Array[GO])
case class GO(id: String, source: String, evidence: String, aspect: String)
