package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.{InputExtension, OTConfig}
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.IOResourceConfig
import org.apache.spark.sql.functions.{array, array_except, array_union, coalesce, col, collect_set, explode, translate, trim, typedLit, when}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DrugExtensions extends LazyLogging {

  /**
    * processExtensions extracts a subset of extension files provided and prepares them for reading into dataframes.
    * @param extentionType name of extension to filter for, eg synonym or cross-reference
    * @param extensions list of all custom extension files provided in config
    * @return filtered list of extensions in format ready to be read into dataframes.
    */
  def processExtensions(extentionType: String,
                        extensions: Seq[InputExtension]): Seq[IOResourceConfig] = {
    extensions
      .filter(_.extensionType equalsIgnoreCase extentionType)
      .flatMap(ext => {
        ext.path match {
          case isJsonPath if isJsonPath.endsWith("json") => Some(IOResourceConfig("json", ext.path))
          case nonJsonPath =>
            logger.error(s"Unable to process extension file $nonJsonPath")
            None
        }
      })
  }

  /**
    *
    * @param moleculeDf basic molecule dataframe generated from ChEMBL inputs in Molecule.scala
    * @param config configuration object for extracting necessary input files
    * @param sparkSession
    * @return a DataFrame with additional synonyms added to the Molecule dataframe as specified by the input files
    *         provided in the configuration.
    */
  def synonymExtensions(moleculeDf: DataFrame, config: Seq[InputExtension])(
      implicit sparkSession: SparkSession): DataFrame = {
    // get extensions from config and read into dataframes.
    val synonymExtensions: Seq[IOResourceConfig] =
      DrugExtensions.processExtensions("synonym", config)
    logger.debug(s"Found ${synonymExtensions.size} synonym extensions.")

    val synonymExtensionDataframes: Iterable[DataFrame] =
      Helpers.readFrom(Helpers.seqToIOResourceConfigMap(synonymExtensions)).values

    // validate input dataframes and standardise so that they are all in id -> array format.
    logger.debug(s"Standardising ${synonymExtensionDataframes.size} DataFrames of synonyms")
    val synonymDFs: Iterable[DataFrame] = synonymExtensionDataframes.map(standardiseSynonymsDf)

    // add all synonym extensions to molecules
    logger.info("Adding external synonyms to molecule dataframe.")
    synonymDFs.foldLeft(moleculeDf)((mols, syns) => addSynonymsToMolecule(mols, syns))

  }

  /**
    * Helper function to ensure that incoming synonyms do not contain special characters and that
    * synonyms are grouped by the id field.
    * @param synonymDf raw DF provided by user
    * @return standardized dataframe in form id: String, synonyms: array[String]
    */
  private def standardiseSynonymsDf(synonymDf: DataFrame): DataFrame = {
    Helpers.validateDF(Set("id", "synonyms"), synonymDf)
    val synonymStructType: DataType = synonymDf.schema("synonyms").dataType

    val standardise: DataFrame => DataFrame =
      _.withColumn("synonyms",
                   translate(trim(col("synonyms")), "αβγδεζηικλμνξπτυω", "abgdezhiklmnxptuo"))
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

  /**
    *
    * @param molecule dataframe of molecules generated from ChEMBL data
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
      .withColumn("synonyms",
                  array_union(coalesce(col("synIn"), typedLit(Seq.empty[String])), col("synonyms")))
      .drop("synIn", synId)

  }

}
