package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{array, array_sort, arrays_zip, col, collect_list, collect_set, explode, lit, map_concat, split, udf, upper, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class Molecule(chembl_mols: DataFrame, drugbank: DataFrame)(implicit sparkSession: SparkSession)
    extends LazyLogging {

  import sparkSession.implicits._

  /**
    *
    * @param chemblMoleculeRaw of raw molecule inputs
    * @param drugbank drugbank lookup table of chembl_id -> drugbank_id
    * @return dataframe with unwanted fields removed, and basic preprocessing completed.
    */
  def moleculePreprocess(chemblMoleculeRaw: DataFrame, drugbank: DataFrame): DataFrame = {
    //      logger.info("Processing molecule data.")
    val columnsOfInterest = chemblMoleculeRaw
      .select(
        col("molecule_chembl_id").as("id"),
        col("molecule_structures.canonical_smiles").as("canonical_smiles"),
        col("molecule_type").as("type"),
        col("chebi_par_id"),
        col("black_box_warning"),
        col("pref_name"),
        col("cross_references"),
        col("first_approval"),
        col("max_phase").as("max_clinical_trial_phase"),
        col("molecule_hierarchy"),
        col("molecule_synonyms.molecule_synonym").as("mol_synonyms"),
        col("molecule_synonyms.syn_type").as("synonym_type"),
        col("pref_name"),
        col("withdrawn_flag"),
        col("withdrawn_year"),
        col("withdrawn_reason"),
        col("withdrawn_country"),
        col("withdrawn_class")
      )
      .withColumn("black_box_warning", when($"black_box_warning" === 1, true).otherwise(false))
      .withColumn("withdrawn_reason", split(col("withdrawn_reason"), ";"))
      .withColumn("withdrawn_country", split(col("withdrawn_country"), ";"))
      .withColumn("withdrawn_class", split(col("withdrawn_class"), ";"))
      .withColumn("syns", arrays_zip($"mol_synonyms", $"synonym_type"))
      .drop("mol_synonyms", "synonym_type")
      .join(drugbank, Seq("id"), "left_outer")

    columnsOfInterest
  }

  /**
    * Method to group synonyms into sorted sets of trade names and others synonyms.
    *
    * @param preProcessedMolecules df prepared with moleculePreprocess method
    * @return dataframe of `id: String, trade_name: Set[String], synonym: Set[String]`
    */
  def processMoleculeSynonyms(preProcessedMolecules: DataFrame): DataFrame = {
    val synonyms: DataFrame = preProcessedMolecules
      .select($"id", explode($"syns"))
      .withColumn("syn_type", upper($"col.synonym_type"))
      .withColumn("synonym", $"col.mol_synonyms")

    val tradeName = synonyms
      .filter($"syn_type" === "TRADE_NAME")
      .groupBy($"id")
      .agg(collect_set($"synonym").alias("trade_name"))
    val synonym = synonyms
      .filter($"syn_type" =!= "TRADE_NAME")
      .groupBy($"id")
      .agg(collect_set($"synonym").alias("synonym"))

    val full = tradeName
      .join(synonym, Seq("id"), "fullouter")
      // cast nulls to empty arrays
      .withColumn("synonym",
                  when($"synonym".isNull, array().cast("array<string>")).otherwise($"synonym"))
      .withColumn(
        "trade_name",
        when($"trade_name".isNull, array().cast("array<string>")).otherwise($"trade_name"))
      // sort lists
      .withColumn("synonym", array_sort($"synonym"))
      .withColumn("trade_name", array_sort($"trade_name"))
    full
  }

  /**
    * Method to group cross references for each molecule id. Source ids are grouped according to source.
    *
    * @param preProcessedMolecules df prepared with moleculePreprocess methods
    * @return dataframe of `id: String, source: Map(src: String -> ids: Array[String])`
    */
  def processMoleculeCrossReferences(preProcessedMolecules: DataFrame): DataFrame = {

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

    val createReferenceMap = udf(createSrcToReferenceMap _)

    /**
      *
      * @param preProcessedMolecules
      * @return dataframe of: id, map(str, array[str])
      */
    def processChemblCrossReferences(preProcessedMolecules: DataFrame): DataFrame = {
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
        .withColumn("xref", createReferenceMap($"ref1"))
        .drop("refs", "ref1")
    }

    /**
      *
      * @param preProcessedMolecules
      * @return chembl_id, Map(source -> Array[ref])
      */
    def processDrugbankCrossReferences(preProcessedMolecules: DataFrame): DataFrame = {
      // Each drug is likely only to have a single drugbank id, but return an array so that
      // it is compatible with other sources which have multiple references.
      preProcessedMolecules
        .filter($"drugbank_id".isNotNull)
        .select("id", "drugbank_id")
        .groupBy("id")
        .agg(collect_set("drugbank_id").as("drugbank_id"))
        .withColumn("xref", functions.map(lit("drugbank"), $"drugbank_id"))
        .drop("drugbank_id")
    }

    def mergeCrossReferences(ref1: DataFrame, ref2: DataFrame): DataFrame = {
      val ref1x = "x"
      val ref2x = "y"
      val r1 = ref1.withColumnRenamed("refs", ref1x)
      val r2 = ref2.withColumnRenamed("refs", ref2x)
      r1
        .join(r2, Seq("id"), "full_outer")
        .withColumn("xref", map_concat($"x", $"y"))
        .drop("x", "y")
    }

    processChemblCrossReferences(preProcessedMolecules)
  }

}
