package io.opentargets.etl.backend.drug_beta

import io.opentargets.etl.backend.SparkHelpers.{applyFunToColumn, nest, validateDF}
import org.apache.spark.sql.functions.{col, collect_list, explode, lower, size, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Class for preparing mechanism of action section of the drug object.
  *
  * Output structure:
  *
  * id
  * mechanism_of_action
  * --description
  * --action_type
  * --references
  * ---- source
  * ---- ids
  * ---- urls
  * --target_name
  * --target_type
  * --target_components
  * ---- approved_name
  * ---- approved_symbol
  * ---- ensembl
  * number_of_mechanisms_of_action
  *
  * @param mechanismDf: raw data from Chembl
  * @param targetDf: raw data from Chembl
  * @param geneDf: gene parquet file listed under target in configuration
  * @param sparkSession implicit
  */
class MechanismOfAction(mechanismDf: DataFrame, targetDf: DataFrame, geneDf: DataFrame)(implicit sparkSession: SparkSession) {
  import sparkSession.implicits._

  def processMechanismOfAction(): DataFrame = {
    val mechanism = mechanismDf
      .withColumnRenamed("molecule_chembl_id", "id")
      .transform(applyFunToColumn("action_type", _, lower))
      .withColumnRenamed("mechanism_of_action", "description")
    val references = chemblMechanismReferences(mechanism)
    val target = chemblTarget(targetDf, geneDf)

    mechanism
      .join(references, Seq("id"), "outer")
      .join(target, Seq("id"), "outer")
      .drop("mechanism_refs", "record_id", "target_chembl_id")
      .transform(nestMechanismUnderIdAndCollectCount)

  }

  private def nestMechanismUnderIdAndCollectCount(dataFrame: DataFrame): DataFrame = {
    val aggName = "mechanism_of_action"
    val discriminator = "id"
    val df = nest(dataFrame, dataFrame.columns.filterNot(_ == discriminator).toList, aggName)
    df.groupBy(discriminator)
      .agg(collect_list(aggName).as(aggName))
      .withColumn("number_of_mechanisms_of_action", size(col(aggName)))
  }

  private def chemblMechanismReferences(dataFrame: DataFrame): DataFrame = {
    val requiredColumns = Set("id", "mechanism_refs")
    validateDF(requiredColumns, dataFrame)

    dataFrame
      .select($"id", explode($"mechanism_refs"))
      .groupBy("id", "col.ref_type")
      .agg(
        collect_list("col.ref_id").as("ref_id"),
        collect_list("col.ref_url").as("ref_url"))
      .withColumn("references",
        struct(
          $"ref_type".as("source"),
          $"ref_id".as("ids"),
          $"ref_url".as("urls")))
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
    val targetDf = target.transform(applyFunToColumn("target_components", _, explode))
      .filter($"target_components.accession".isNotNull)
      .transform(applyFunToColumn("target_type", _, lower))
      .select($"pref_name".as("target_name"), $"target_components.accession".as("uniprot_id"), $"target_type", $"target_chembl_id".as("id"))
    val genes = gene.select(geneCols.map(col): _*)

    targetDf.join(genes, Seq("uniprot_id"), "left_outer")
      .withColumn("target_components", struct(
        $"approved_name",
        $"approved_symbol",
        $"ensembl_gene_id".as("ensembl")
      ))
      .groupBy("id", "target_name", "target_type")
      .agg(collect_list("target_components").as("target_components"))

  }
}
