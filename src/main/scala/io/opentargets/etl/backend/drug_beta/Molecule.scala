package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{
  array,
  array_sort,
  arrays_zip,
  col,
  collect_list,
  collect_set,
  explode,
  lit,
  map_concat,
  split,
  udf,
  upper,
  when
}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class Molecule(moleculeRaw: DataFrame, drugbankRaw: DataFrame)(implicit sparkSession: SparkSession)
    extends LazyLogging
    with Serializable {

  import sparkSession.implicits._
  import io.opentargets.etl.backend.spark.Helpers.nest

  private val XREF_COLUMN_NAME = "xref"

  def processMolecules: DataFrame = {
    logger.info("Processing molecules.")
    val mols: DataFrame = moleculePreprocess(moleculeRaw, drugbankRaw)
    val synonyms: DataFrame = processMoleculeSynonyms(mols)
    val crossReferences: DataFrame = processMoleculeCrossReferences(mols)
    val hierarchy: DataFrame = processMoleculeHierarchy(mols)
    mols
      .drop("cross_references", "syns", "chebi_par_id", "drugbank_id", "molecule_hierarchy")
      .join(synonyms, Seq("id"), "left_outer")
      .join(crossReferences, Seq("id"), "left_outer")
      .join(hierarchy, Seq("id"), "left_outer")

  }

  /**
    *
    * @param chemblMoleculeRaw of raw molecule inputs
    * @param drugbank drugbank lookup table of chembl_id -> drugbank_id
    * @return dataframe with unwanted fields removed, and basic preprocessing completed.
    */
  private def moleculePreprocess(chemblMoleculeRaw: DataFrame, drugbank: DataFrame): DataFrame = {
    //      logger.info("Processing molecule data.")
    val columnsOfInterest = chemblMoleculeRaw
      .select(
        col("molecule_chembl_id").as("id"),
        col("molecule_structures.canonical_smiles").as("canonicalSmiles"),
        col("molecule_type").as("drugType"),
        col("chebi_par_id"),
        col("black_box_warning").as("blackBoxWarning"),
        col("pref_name").as("name"),
        col("cross_references"),
        col("first_approval").as("yearOfFirstApproval"),
        col("max_phase").as("maximumClinicalTrialPhase"),
        col("molecule_hierarchy"),
        col("molecule_synonyms.molecule_synonym").as("mol_synonyms"),
        col("molecule_synonyms.syn_type").as("synonym_type"),
        col("withdrawn_flag").as("hasBeenWithdrawn"),
        col("withdrawn_year"),
        col("withdrawn_reason"),
        col("withdrawn_country"),
        col("withdrawn_class")
      )
      .withColumn("blackBoxWarning", when($"blackBoxWarning" === 1, true).otherwise(false))
      .withColumn("withdrawn_reason", split(col("withdrawn_reason"), ";"))
      .withColumn("withdrawn_country", split(col("withdrawn_country"), ";"))
      .withColumn("withdrawn_class", split(col("withdrawn_class"), ";"))
      .withColumn("syns", arrays_zip($"mol_synonyms", $"synonym_type"))
      .drop("mol_synonyms", "synonym_type", "withdrawn_reason")
      .transform(processWithdrawnNotices)
      .join(drugbank, Seq("id"), "left_outer")

    columnsOfInterest
  }

  private def processWithdrawnNotices(dataFrame: DataFrame): DataFrame = {
    val df = dataFrame
      .withColumnRenamed("withdrawn_country", "countries")
      .withColumnRenamed("withdrawn_class", "classes")
      .withColumnRenamed("withdrawn_year", "year")
    nest(df, List("countries", "classes", "year"), "withdrawnNotice")
  }

  /**
    * Method to group synonyms into sorted sets of trade names and others synonyms.
    *
    * @param preProcessedMolecules df prepared with moleculePreprocess method
    * @return dataframe of `id: String, tradeName: Set[String], synonym: Set[String]`
    */
  private def processMoleculeSynonyms(preProcessedMolecules: DataFrame): DataFrame = {
    val synonyms: DataFrame = preProcessedMolecules
      .select($"id", explode($"syns"))
      .withColumn("syn_type", upper($"col.synonym_type"))
      .withColumn("synonym", $"col.mol_synonyms")

    val tradeName = synonyms
      .filter($"syn_type" === "TRADE_NAME")
      .groupBy($"id")
      .agg(collect_set($"synonym").alias("tradeNames"))
    val synonym = synonyms
      .filter($"syn_type" =!= "TRADE_NAME")
      .groupBy($"id")
      .agg(collect_set($"synonym").alias("synonyms"))

    val groupings = Seq("synonyms", "tradeNames")
    val full = tradeName
      .join(synonym, Seq("id"), "fullouter")

    groupings.foldLeft(full)(
      (df, colName) =>
        df.withColumn(colName,
                      when(col(colName).isNull, array().cast("array<string>"))
                        .otherwise(array_sort(col(colName)))))

  }

  /**
    * Group all child molecules by chembl_id
    * @param preProcessedMolecules df produced by this.moleculePreprocess
    * @return dataframe of two columns `id`: Str, `child_chembl_ids`: Array[Str]
    */
  private def processMoleculeHierarchy(preProcessedMolecules: DataFrame): DataFrame = {
    preProcessedMolecules
      .select($"id", $"molecule_hierarchy.parent_chembl_id".as("parent_id"))
      .filter($"id" =!= $"parent_id")
      .groupBy("parent_id")
      .agg(collect_list($"id").as("childChemblIds"))
      .withColumnRenamed("parent_id", "id")
  }

  /**
    * Method to group cross references for each molecule id. Source ids are grouped according to source.
    *
    * @param preProcessedMolecules df prepared with moleculePreprocess methods
    * @return dataframe of `id: String, source: Map(src: String -> ids: Array[String])`
    */
  private def processMoleculeCrossReferences(preProcessedMolecules: DataFrame): DataFrame = {

    val chemblCrossReferences = processChemblCrossReferences(preProcessedMolecules)
    val singletonRefs: List[Tuple2[String, String]] = List(
      ("drugbank_id", "drugbank"),
      ("chebi_par_id", "chEBI")
    )

    singletonRefs
      .map(src => processSingletonCrossReferences(preProcessedMolecules, src._1, src._2))
      .foldLeft(chemblCrossReferences)((agg, a) => mergeCrossReferenceMaps(agg, a))
      .withColumnRenamed("xref", "crossReferences")
  }

  /**
    *
    * @param preProcessedMolecules generated by `molecule preprocess`
    * @return dataframe of: id, map(str, array[str])
    */
  private def processChemblCrossReferences(preProcessedMolecules: DataFrame): DataFrame = {

    val createReferenceMap: UserDefinedFunction = udf(Molecule.createSrcToReferenceMap _)
    // [id: str, refs: Array[src, ref_id]
    val chemblXR = preProcessedMolecules
      .select($"id",
              explode(arrays_zip($"cross_references.xref_id".as("ref_id"),
                                 $"cross_references.xref_src".as("ref_source"))).as("sources"))
      .withColumn("ref_id", $"sources.xref_id")
      .withColumn("ref_src", $"sources.xref_src")
      .withColumn("refs", array($"ref_src", $"ref_id").as("refs"))
      .drop("sources", "ref_id", "ref_src")

    chemblXR
      .groupBy("id")
      .agg(collect_list($"refs").as("ref1"))
      .withColumn(XREF_COLUMN_NAME, createReferenceMap($"ref1"))
      .drop("refs", "ref1")
  }

  /**
    * Helper method to link singleton references to a ChEMBL id and return a cross reference map.
    *
    * Although the referenceIdColumn must be a singleton (str, int, long, etc) the method returns a map with an
    * array of values so that types are compatible with sources that have several references.
    *
    * @param preProcessedMolecules dataframe prepared by `moleculePreprocess` method
    * @param referenceIdColumn column in preProcessed molecules to use as reference ids. Must only be single values.
    * @param source name of source used as key in returned map
    * @return chembl_id, Map(source -> Array[ref])
    */
  private def processSingletonCrossReferences(preProcessedMolecules: DataFrame,
                                              referenceIdColumn: String,
                                              source: String): DataFrame = {
    preProcessedMolecules
      .filter(col(referenceIdColumn).isNotNull)
      .select($"id", col(referenceIdColumn).cast("string"))
      .groupBy("id")
      .agg(collect_set(referenceIdColumn).as(referenceIdColumn))
      .withColumn("xref", functions.map(lit(source), col(referenceIdColumn)))
      .drop(referenceIdColumn)
  }

  /**
    * Helper method which takes in two dataframes of cross references and returns a single dataframe with the
    * cross reference maps merged.
    *
    * @param ref1 dataframe with columns `id` and `xref`
    * @param ref2 dataframe with columns `id` and `xref`
    * @return
    */
  private def mergeCrossReferenceMaps(ref1: DataFrame, ref2: DataFrame): DataFrame = {
    val ref1x = "x"
    val ref2x = "y"
    val r1 = ref1.withColumnRenamed(XREF_COLUMN_NAME, ref1x)
    val r2 = ref2.withColumnRenamed(XREF_COLUMN_NAME, ref2x)
    r1.join(r2, Seq("id"), "full_outer")
      .withColumn(XREF_COLUMN_NAME, map_concat($"x", $"y"))
      .drop("x", "y")
  }

}

object Molecule {

  /**
    * Helper function to created maps where references are grouped by source.
    *
    * @param refSrcAndId sequence on inner pairs (src, id). Need to use seq rather than tuple
    *                    for Spark to handle it properly.
    * @return Map[Source, Reference]
    */
  def createSrcToReferenceMap(refSrcAndId: Seq[Seq[String]]): Map[String, Seq[String]] = {
    // (src, id)
    val i: Seq[(String, String)] =
      refSrcAndId.filter(_.size == 2).map(s => (s.head, s(1)))

    i.groupBy(k => k._1).map(v => (v._1, v._2.map(_._2)))
  }

}
