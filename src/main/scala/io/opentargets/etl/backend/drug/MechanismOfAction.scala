package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.validateDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Object for preparing mechanism of action section of the drug object.
  *
  * Output structure:
  *
  * id
  * mechanismsOfAction
  * -- rows
  * ---- mechanismOfAction
  * ---- references
  * ------ source
  * ------ ids
  * ------ urls
  * ---- targets
  * ---- targetName
  * -- uniqueActiontype
  * -- unqueTargetType
  */
object MechanismOfAction extends LazyLogging {

  /** @param mechanismDf: raw data from Chembl
    * @param targetDf: raw data from Chembl
    * @param geneDf: gene parquet file listed under target in configuration
    * @param sparkSession implicit
    */
  def apply(mechanismDf: DataFrame, targetDf: DataFrame, geneDf: DataFrame)(implicit
      sparkSession: SparkSession
  ): DataFrame = {

    logger.info("Processing mechanisms of action")
    val mechanism = mechanismDf
      .withColumnRenamed("molecule_chembl_id", "id")
      .withColumnRenamed("mechanism_of_action", "mechanismOfAction")
      .withColumnRenamed("action_type", "actionType")
      .withColumn("chemblIds", col("_metadata.all_molecule_chembl_ids"))
      .drop("_metadata", "parent_molecule_chembl_id")

    val references = chemblMechanismReferences(mechanism)
    val target = chemblTarget(targetDf, geneDf)

    mechanism
      .join(references, Seq("id"), "outer")
      .join(target, Seq("target_chembl_id"), "outer")
      .drop("mechanism_refs", "record_id", "target_chembl_id")
      // filter to remove rows which don't require further processing. Including them results in a bug in the API.
      .filter(
        """
          |mechanismOfAction is not null
          |or references is not null
          |or targetName is not null
          |or targets is not null
          |""".stripMargin
      )
      .drop("id")
      .filter("chemblIds is not null")
      .distinct
      .transform(consolidateDuplicateReferences)

  }

  private def consolidateDuplicateReferences(df: DataFrame): DataFrame = {
    val cols = df.columns.filter(_ != "references")
    df.groupBy(cols.head, cols.tail: _*)
      .agg(collect_set("references").as("r"))
      .withColumn("references", flatten(col("r")))
      .drop("r")
  }

  private def chemblMechanismReferences(dataFrame: DataFrame): DataFrame = {
    val requiredColumns = Set("id", "mechanism_refs")
    validateDF(requiredColumns, dataFrame)

    dataFrame
      .select(col("id"), explode(col("mechanism_refs")))
      .groupBy("id", "col.ref_type")
      .agg(collect_list("col.ref_id").as("ref_id"), collect_list("col.ref_url").as("ref_url"))
      .withColumn(
        "references",
        struct(col("ref_type").as("source"), col("ref_id").as("ids"), col("ref_url").as("urls"))
      )
      .groupBy("id")
      .agg(collect_list("references").as("references"))
  }

  private def chemblTarget(target: DataFrame, gene: DataFrame): DataFrame = {
    val targetCols = Set("target_components", "pref_name", "target_type", "target_chembl_id")
    val geneCols = List(col("id").as("geneId"), explode(col("proteinIds.id")).as("uniprot_id"))

    // validate incoming dataframes
    validateDF(targetCols, target)

    // get rid of entries where target components is none
    val targetDf = target
      .withColumn("target_components", explode(col("target_components")))
      .filter(col("target_components.accession").isNotNull)
      .select(
        col("pref_name").as("targetName"),
        col("target_components.accession").as("uniprot_id"),
        lower(col("target_type")).as("targetType"),
        col("target_chembl_id")
      )
    val genes = gene.select(geneCols: _*)

    targetDf
      .join(
        genes,
        targetDf("uniprot_id") === genes("uniprot_id") || targetDf("uniprot_id") === genes(
          "geneId"
        ),
        "left_outer"
      )
      .groupBy("target_chembl_id", "targetName", "targetType")
      .agg(array_distinct(collect_list("geneId")).as("targets"))
  }
}
