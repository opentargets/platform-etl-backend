package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{
  array,
  array_sort,
  arrays_zip,
  col,
  collect_list,
  collect_set,
  explode,
  map_from_arrays,
  split,
  udf,
  upper,
  when
}

/**
  * This step will eventually replace the existing Drug step.
  *
  * It incorporates processing which was previously done in the `data-pipeline` project and consolidates all the logic in
  * this class.
  */
object MoleculeHelper extends LazyLogging {

  /**
    * Break array of synonyms for molecule into two categories, 'trade_name' and other
    * types.
    *
    * @param synonym  array of synonyms
    * @param syn_type same length as `synonym` with a corresponding type
    * @return tuple (trade_names, synonyms)
    */
  def processSynonyms(synonym: Array[String],
                      syn_type: Array[String]): (Array[String], Array[String]) = {
    assert(synonym.length equals syn_type.length)
    val results = synonym
      .zip(syn_type)
      .partition(_._2 equalsIgnoreCase "trade_name")
    (results._1.map(_._1), results._2.map(_._1))
  }

  implicit class MoleculeHelpers(dataframe: DataFrame)(implicit ss: SparkSession) {
    import ss.implicits._

    /**
      * Method to group synonyms into sorted sets of trade names and others synonyms.
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
      * @param preProcessedMolecules df prepared with moleculePreprocess methods
      * @return dataframe of `id: String, source: Map(src: String -> ids: Array[String])`
      */
    def processMoleculeCrossReferences(preProcessedMolecules: DataFrame): DataFrame = {
      def fun(refSrcAndId: Seq[Seq[String]]): Map[String, Seq[String]] = {
        // (src, id)
        val i: Seq[(String, String)] =
          refSrcAndId.filter(_.size == 2).map(s => (s.head, s(1)))

        i.groupBy(k => k._1).map(v => (v._1, v._2.map(_._2)))
      }

      val createReferenceMap = udf(fun _)

      val xr = preProcessedMolecules
        .select($"id",
                explode(arrays_zip($"cross_references.xref_id".as("ref_id"),
                                   $"cross_references.xref_src".as("ref_source"))).as("sources"))
        .withColumn("ref_id", $"sources.xref_id")
        .withColumn("ref_src", $"sources.xref_src")
        .withColumn("refs", array($"ref_src", $"ref_id").as("refs"))
        .drop("sources", "ref_id", "ref_src")

        xr.groupBy("id")
        .agg(collect_list($"refs").as("ref1"))
        .withColumn("xref", createReferenceMap($"ref1"))
        .drop("refs", "ref1")
    }
  }

}

object DrugBeta extends LazyLogging {

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val common = context.configuration.common

    logger.info("Loading raw inputs for Drug beta step.")
    val mappedInputs = Map(
      "indication" -> IOResourceConfig(common.inputs.drugChemblIndication.format,
                                       common.inputs.drugChemblIndication.path),
      "mechanism" -> IOResourceConfig(common.inputs.drugChemblMechanism.format,
                                      common.inputs.drugChemblMechanism.path),
      "molecule" -> IOResourceConfig(common.inputs.drugChemblMolecule.format,
                                     common.inputs.drugChemblMolecule.path),
      "target" -> IOResourceConfig(common.inputs.drugChemblTarget.format,
                                   common.inputs.drugChemblTarget.path)
    )

    val inputDataFrames = SparkHelpers.readFrom(mappedInputs)

    val moleculeDf: DataFrame = inputDataFrames("molecule")
    val mechanismDf: DataFrame = inputDataFrames("mechanism")
    val indicationDf: DataFrame = inputDataFrames("indication")
    val targetDf: DataFrame = inputDataFrames("target")

    logger.info("Raw inputs for Drug beta loaded.")

    /**
      *
      * @param df of raw molecule inputs
      * @return dataframe with unwanted fields removed, and basic preprocessing completed.
      */
    def moleculePreprocess(df: DataFrame): DataFrame = {
      //      logger.info("Processing molecule data.")
      val columnsOfInterest = df
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

      columnsOfInterest
    }

    def mechanismPreprocess(df: DataFrame): DataFrame = ???

    def indicationPreprocess(df: DataFrame): DataFrame = ???

    def targetPreprocess(df: DataFrame): DataFrame = ???

  }

}
