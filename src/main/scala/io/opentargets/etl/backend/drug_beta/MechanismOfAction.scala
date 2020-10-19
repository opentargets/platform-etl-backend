package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{nest, validateDF}
import org.apache.spark.sql.functions.{
  size,
  array_distinct,
  col,
  collect_list,
  collect_set,
  explode,
  isnull,
  lower,
  struct,
  when
}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Class for preparing mechanism of action section of the drug object.
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
  * @param mechanismDf: raw data from Chembl
  * @param targetDf: raw data from Chembl
  * @param geneDf: gene parquet file listed under target in configuration
  * @param sparkSession implicit
  */
class MechanismOfAction(mechanismDf: DataFrame, targetDf: DataFrame, geneDf: DataFrame)(
    implicit sparkSession: SparkSession)
    extends LazyLogging {
  import sparkSession.implicits._

  def processMechanismOfAction: DataFrame = {
    logger.info("Processing mechanisms of action")
    val mechanism = mechanismDf
      .withColumnRenamed("molecule_chembl_id", "id")
      .withColumnRenamed("mechanism_of_action", "mechanismOfAction")
    val references = chemblMechanismReferences(mechanism)
    val target = chemblTarget(targetDf, geneDf)

    mechanism
      .join(references, Seq("id"), "outer")
      .join(target, Seq("target_chembl_id"), "outer")
      .drop("mechanism_refs", "record_id", "target_chembl_id")
      .transform(nest(_: DataFrame,
                      List("mechanismOfAction", "references", "targetName", "targets"),
                      "rows"))
      .groupBy("id")
      .agg(collect_list("rows") as "rows",
           collect_set("action_type") as "uniqueActionTypes",
           collect_set("target_type") as "uniqueTargetTypes")
      .transform(nest(_: DataFrame,
                      List("rows", "uniqueActionTypes", "uniqueTargetTypes"),
                      "mechanismsOfAction"))
  }

  private def chemblMechanismReferences(dataFrame: DataFrame): DataFrame = {
    val requiredColumns = Set("id", "mechanism_refs")
    validateDF(requiredColumns, dataFrame)

    dataFrame
      .select($"id", explode($"mechanism_refs"))
      .groupBy("id", "col.ref_type")
      .agg(collect_list("col.ref_id").as("ref_id"), collect_list("col.ref_url").as("ref_url"))
      .withColumn("references",
                  struct($"ref_type".as("source"), $"ref_id".as("ids"), $"ref_url".as("urls")))
      .groupBy("id")
      .agg(collect_list("references").as("references"))
  }

  private def chemblTarget(target: DataFrame, gene: DataFrame): DataFrame = {
    val targetCols = Set("target_components", "pref_name", "target_type", "target_chembl_id")
    val geneCols = List("ensembl_gene_id", "uniprot_id", "approved_name", "approved_symbol")

    // validate incoming dataframes
    validateDF(targetCols, target)
    validateDF(geneCols.toSet, gene)

    // get rid of entries where target components is none
    val targetDf = target
      .withColumn("target_components", explode($"target_components"))
      .filter($"target_components.accession".isNotNull)
      .withColumn("target_type", lower($"target_type"))
      .select($"pref_name".as("targetName"),
        $"target_components.accession".as("uniprot_id"),
        $"target_type",
        $"target_chembl_id")
    val genes = gene.select(geneCols.map(col): _*)

    targetDf
      .join(genes, Seq("uniprot_id"), "left_outer")
      .groupBy("target_chembl_id", "targetName", "target_type")
      .agg(array_distinct(collect_list("ensembl_gene_id")).as("targets"))
  }

}
