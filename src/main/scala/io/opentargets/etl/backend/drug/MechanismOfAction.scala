package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{nest, validateDF}
import org.apache.spark.sql.functions.{
  array,
  array_distinct,
  array_union,
  coalesce,
  col,
  collect_list,
  collect_set,
  explode,
  lower,
  struct,
  typedLit
}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Object for preparing mechanism of action section of the drug object.
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
  *

  */
object MechanismOfAction extends LazyLogging {

  /**
    *
    * @param mechanismDf: raw data from Chembl
    * @param targetDf: raw data from Chembl
    * @param geneDf: gene parquet file listed under target in configuration
    * @param sparkSession implicit
    */
  def apply(mechanismDf: DataFrame, targetDf: DataFrame, geneDf: DataFrame, molecule: DataFrame)(
      implicit sparkSession: SparkSession): DataFrame = {

    logger.info("Processing mechanisms of action")
    val mechanism = mechanismDf
      .withColumnRenamed("molecule_chembl_id", "id")
      .withColumnRenamed("mechanism_of_action", "mechanismOfAction")
      .withColumnRenamed("action_type", "actionType")
    val references = chemblMechanismReferences(mechanism)
    val target = chemblTarget(targetDf, geneDf)
    val hierarchy = chemblHierarchy(molecule)

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
      .join(hierarchy, Seq("id"), "left_outer")
      .drop("id")
      .dropDuplicates("chemblIds")
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
        struct(col("ref_type").as("source"), col("ref_id").as("ids"), col("ref_url").as("urls")))
      .groupBy("id")
      .agg(collect_list("references").as("references"))
  }

  private def chemblTarget(target: DataFrame, gene: DataFrame): DataFrame = {
    val targetCols = Set("target_components", "pref_name", "target_type", "target_chembl_id")
    val geneCols = List(col("id").as("geneId"), col("proteinAnnotations.id").as("uniprot_id"))

    // validate incoming dataframes
    validateDF(targetCols, target)

    // get rid of entries where target components is none
    val targetDf = target
      .withColumn("target_components", explode(col("target_components")))
      .filter(col("target_components.accession").isNotNull)
      .select(col("pref_name").as("targetName"),
              col("target_components.accession").as("uniprot_id"),
              lower(col("target_type")).as("targetType"),
              col("target_chembl_id"))
    val genes = gene.select(geneCols: _*)

    targetDf
      .join(genes, Seq("uniprot_id"), "left_outer")
      .groupBy("target_chembl_id", "targetName", "targetType")
      .agg(array_distinct(collect_list("geneId")).as("targets"))
  }

  def chemblHierarchy(molecule: DataFrame): DataFrame = {
    molecule
      .withColumn("children", coalesce(col("childChemblIds"), typedLit(Array.empty)))
      .withColumn("chemblIds", array_union(array(col("id"), col("parentId")), col("children")))
      .select(col("id"), explode(col("chemblIds")).as("x"))
      .groupBy("id")
      .agg(collect_set("x").as("chemblIds"))
      .dropDuplicates()
  }

}
