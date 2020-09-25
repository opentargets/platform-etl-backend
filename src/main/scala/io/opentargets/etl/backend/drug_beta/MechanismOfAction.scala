package io.opentargets.etl.backend.drug_beta

import io.opentargets.etl.backend.SparkHelpers.applyFunToColumn
import org.apache.spark.sql.functions.{col, collect_list, explode, lower, struct, transform}
import org.apache.spark.sql.{Column, ColumnName, DataFrame, SparkSession}


/**
  * Input from mechanism df
  * root
  * |-- action_type: string (nullable = true)           // to lower case
  * |-- mechanism_of_action: string (nullable = true)   // (rename description)
  * |-- mechanism_refs: array (nullable = true)         // group by type and have array of id, url (rename references)
  * |    |-- element: struct (containsNull = true)
  * |    |    |-- ref_id: string (nullable = true)
  * |    |    |-- ref_type: string (nullable = true)
  * |    |    |-- ref_url: string (nullable = true)
  * |-- molecule_chembl_id: string (nullable = true)
  * |-- record_id: long (nullable = true)
  * |-- target_chembl_id: string (nullable = true)
  *
  * Input from Target df
  * root
  * |-- pref_name: string (nullable = true)
  * |-- target_chembl_id: string (nullable = true)
  * |-- target_components: array (nullable = true)
  * |    |-- element: struct (containsNull = true)
  * |    |    |-- accession: string (nullable = true)
  * |    |    |-- component_description: string (nullable = true)
  * |    |    |-- component_id: long (nullable = true)
  * |    |    |-- component_type: string (nullable = true)
  * |    |    |-- relationship: string (nullable = true)
  * |    |    |-- target_component_synonyms: array (nullable = true)
  * |    |    |    |-- element: struct (containsNull = true)
  * |    |    |    |    |-- component_synonym: string (nullable = true)
  * |    |    |    |    |-- syn_type: string (nullable = true)
  * |    |    |-- target_component_xrefs: array (nullable = true)
  * |    |    |    |-- element: struct (containsNull = true)
  * |    |    |    |    |-- xref_id: string (nullable = true)
  * |    |    |    |    |-- xref_name: string (nullable = true)
  * |    |    |    |    |-- xref_src_db: string (nullable = true)
  * |    |    |    |    |-- xref_src_url: string (nullable = true)
  * |    |    |    |    |-- xref_url: string (nullable = true)
  * |-- target_type: string (nullable = true)
  *
  * expected output:
  * mechanisms_of_action: array (nullable = true)
  * |    |-- element: struct (containsNull = true)
  * |    |    |-- action_type: string (nullable = true)
  * |    |    |-- description: string (nullable = true)
  * |    |    |-- references: array (nullable = true)
  * |    |    |    |-- element: struct (containsNull = true)
  * |    |    |    |    |-- ids: array (nullable = true)
  * |    |    |    |    |    |-- element: string (containsNull = true)
  * |    |    |    |    |-- source: string (nullable = true)
  * |    |    |    |    |-- urls: array (nullable = true)
  * |    |    |    |    |    |-- element: string (containsNull = true)
  * |    |    |-- target_components: array (nullable = true)
  * |    |    |    |-- element: struct (containsNull = true)
  * |    |    |    |    |-- approved_name: string (nullable = true)
  * |    |    |    |    |-- approved_symbol: string (nullable = true)
  * |    |    |    |    |-- ensembl: string (nullable = true)
  * |    |    |-- target_name: string (nullable = true)
  * |    |    |-- target_type: string (nullable = true)
  *
  * @param mechanismDf
  * @param sparkSession
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
      .join(references, Seq("id"), "left_outer")
      .join(target, $"id" === target.col("target_chembl_id"), "left_outer")

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
      .select($"pref_name".as("target_name"), $"target_components.accession".as("uniprot_id"), $"target_type", $"target_chembl_id")
    val genes = gene.select(geneCols.map(col): _*)

    targetDf.join(genes, Seq("uniprot_id"), "left_outer")
      .withColumn("target_components", struct(
        $"approved_name",
        $"approved_symbol",
        $"ensembl_gene_id".as("ensembl")
      ))
      .groupBy("target_chembl_id", "target_name", "target_type")
      .agg(collect_list("target_components").as("target_components"))

  }

  private def validateDF(requiredColumns: Set[String], dataFrame: DataFrame): Unit = {
    lazy val msg = s"One or more required columns (${requiredColumns.mkString(",")}) not found in dataFrame columns: ${dataFrame.columns.mkString(",")}"
    assert(dataFrame.columns.forall(requiredColumns.contains), msg)
  }
}
