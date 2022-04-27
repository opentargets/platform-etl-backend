package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.InputExtension
import io.opentargets.etl.backend.spark.IoHelpers.seqToIOResourceConfigMap
import io.opentargets.etl.backend.spark.{Helpers, IOResourceConfig, IoHelpers}
import org.apache.spark.sql.functions.{
  array_distinct,
  array_except,
  array_union,
  coalesce,
  col,
  collect_list,
  collect_set,
  explode,
  map_entries,
  map_from_arrays,
  translate,
  trim,
  typedLit
}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DrugExtensions extends LazyLogging {

  def apply(molCombined: DataFrame, drugExtensions: Seq[InputExtension])(implicit
      sparkSession: SparkSession
  ): DataFrame = {
    val extensionMethods =
      Seq(synonymExtensions(_, drugExtensions), crossReferenceExtensions(_, drugExtensions))
    extensionMethods.foldLeft(molCombined)((mol, extFun) => extFun(mol))
  }

  private def crossReferenceExtensions(moleculeDf: DataFrame, config: Seq[InputExtension])(implicit
      sparkSession: SparkSession
  ): DataFrame = {
    // get extensions from config and read into dataframes.
    val xrefExtensions: Seq[IOResourceConfig] =
      DrugExtensions.groupExtensionByType("cross-references", config)
    logger.debug(s"Found ${xrefExtensions.size} cross reference extension files.")

    val extensionDataFrames: Iterable[DataFrame] =
      IoHelpers.readFrom(seqToIOResourceConfigMap(xrefExtensions)) map (_._2.data)

    // add all synonym extensions to molecules
    logger.info("Adding external cross references to molecule dataframe.")
    extensionDataFrames.foldLeft(moleculeDf)((mols, refs) =>
      addCrossReferenceToMolecule(mols, refs)
    )
  }

  /** @param moleculeDf basic molecule dataframe generated from ChEMBL inputs in Molecule.scala
    * @param config configuration object for extracting necessary input files
    * @param sparkSession running spark session
    * @return a DataFrame with additional synonyms added to the Molecule dataframe as specified by the input files
    *         provided in the configuration.
    */
  private def synonymExtensions(moleculeDf: DataFrame, config: Seq[InputExtension])(implicit
      sparkSession: SparkSession
  ): DataFrame = {
    // get extensions from config and read into dataframes.
    val synonymExtensions: Seq[IOResourceConfig] =
      DrugExtensions.groupExtensionByType("synonym", config)
    logger.debug(s"Found ${synonymExtensions.size} synonym extensions.")

    val synonymExtensionDataframes: Iterable[DataFrame] =
      IoHelpers.readFrom(seqToIOResourceConfigMap(synonymExtensions)) map (_._2.data)

    // validate input dataframes and standardise so that they are all in id -> array format.
    logger.debug(s"Standardising ${synonymExtensionDataframes.size} DataFrames of synonyms")
    val synonymDFs: Iterable[DataFrame] = synonymExtensionDataframes.map(standardiseSynonymsDf)

    // add all synonym extensions to molecules
    logger.info("Adding external synonyms to molecule dataframe.")
    synonymDFs.foldLeft(moleculeDf)((mols, syns) => addSynonymsToMolecule(mols, syns))

  }

  private def addCrossReferenceToMolecule(mols: DataFrame, references: DataFrame): DataFrame = {
    logger.debug("Trying to add cross reference to molecule.")
    // process references DF so that the xrefs are in a map.
    val newXrefs: DataFrame = references
      .withColumnRenamed("source", "key")
      .groupBy("id", "key")
      .agg(collect_list("reference").as("vNew"))

    // add new xref column to molecule
    val molXrefs = mols
      .select(col("id"), explode(map_entries(col("crossReferences"))).as("m"))
      .select(col("id"), col("m.key").as("key"), col("m.value").as("vOld"))

    val joinedRefArrays = molXrefs
      .join(newXrefs, Seq("id", "key"), "full_outer")
      .withColumn(
        "values",
        array_distinct(
          array_union(
            coalesce(col("vNew"), typedLit[Array[String]](Array.empty)),
            coalesce(col("vOld"), typedLit[Array[String]](Array.empty))
          )
        )
      )
      .drop("vOld", "vNew")

    val refMap = joinedRefArrays
      .groupBy("id")
      .agg(
        map_from_arrays(
          collect_list(col("key")),
          collect_list(col("values"))
        ).as("xrefs")
      )

    mols
      .join(refMap, Seq("id"), "full_outer")
      .drop("crossReferences")
      .withColumnRenamed("xrefs", "crossReferences")

  }

  /** processExtensions extracts a subset of extension files provided and prepares them for reading into dataframes.
    *
    * @param extentionType name of extension to filter for, eg synonym or cross-reference
    * @param extensions list of all custom extension files provided in config
    * @return filtered list of extensions in format ready to be read into dataframes.
    */
  private def groupExtensionByType(
      extentionType: String,
      extensions: Seq[InputExtension]
  ): Seq[IOResourceConfig] =
    extensions
      .withFilter(_.extensionType equalsIgnoreCase extentionType)
      .map(_.input)

  /** Helper function to ensure that incoming synonyms do not contain special characters and that
    * synonyms are grouped by the id field.
    * @param synonymDf raw DF provided by user
    * @return standardized dataframe in form id: String, synonyms: array[String]
    */
  private def standardiseSynonymsDf(synonymDf: DataFrame): DataFrame = {
    Helpers.validateDF(Set("id", "synonyms"), synonymDf)
    val synonymStructType: DataType = synonymDf.schema("synonyms").dataType

    val standardise: DataFrame => DataFrame =
      _.withColumn(
        "synonyms",
        translate(trim(col("synonyms")), "αβγδεζηικλμνξπτυω", "abgdezhiklmnxptuo")
      )
        .groupBy("id")
        .agg(collect_set(col("synonyms")).as("synonyms"))

    synonymStructType match {
      case _: ArrayType =>
        synonymDf
          .select(col("id"), explode(col("synonyms")).as("synonyms"))
          .transform(standardise)
      case _: StringType =>
        synonymDf.transform(standardise)
      case s =>
        logger.error(s"Unsupported synonym data type $s. Inputs must be either String or Array")
        synonymDf
    }

  }

  /** @param molecule dataframe of molecules generated from ChEMBL data
    * @param synonyms must have been standardised!
    * @return moleculeDf with additional synonyms as specified in `synonyms`
    */
  private def addSynonymsToMolecule(molecule: DataFrame, synonyms: DataFrame): DataFrame = {
    // prevent ambiguous columns on join
    val synId = "extensionId"
    val synIn = synonyms
      .select(col("synonyms").as("synIn"), col("id").as(synId))
    // identify if we're joining on Drugbank_id or chembl_id
    val joinCol = synIn.first.getAs[String](synId) match {
      case db if db.startsWith("D") => "drugbank_id"
      case _                        => "id"
    }
    molecule
      .join(synIn, molecule(joinCol) === synIn(synId), "leftouter")
      // remove new synonyms which are already in trade names
      .withColumn("synIn", array_except(col("synIn"), col("tradeNames")))
      // join remaining new synonyms to existing ones
      .withColumn(
        "synonyms",
        array_union(coalesce(col("synIn"), typedLit(Seq.empty[String])), col("synonyms"))
      )
      .drop("synIn", synId)

  }

}
