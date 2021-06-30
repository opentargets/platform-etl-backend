package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions.{
  array,
  array_contains,
  array_distinct,
  broadcast,
  col,
  collect_list,
  collect_set,
  explode,
  flatten,
  regexp_extract,
  split,
  struct,
  transform,
  translate,
  trim,
  upper
}

object MousePhenotypes extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val mpDF = compute(context)

    val dataframesToSave: IOResources = Map(
      "mousePhenotypes" -> IOResource(mpDF, context.configuration.mousePhenotypes.output)
    )

    IoHelpers.writeTo(dataframesToSave)

  }

  def compute(context: ETLSessionContext)(implicit ss: SparkSession): DataFrame = {
    val config = context.configuration.mousePhenotypes
    val mappedInputs = Map(
      "mp" -> config.mpClasses,
      "reports" -> config.mpReports,
      "orthology" -> config.mpOrthology,
      "categories" -> config.mpCategories,
      "targets" -> config.target
    )
    val inputDataFrame = IoHelpers.readFrom(mappedInputs)

    val mousePhenotypesRawDF = inputDataFrame("mp").data
    val orthologyRawDF = inputDataFrame("orthology").data
      .toDF("human_gene_symbol", "a", "b", "c", "gene_symbol", "gene_id", "phenotypes_raw", "d")
      .drop("a", "b", "c", "d")
    val reportsRawDF = inputDataFrame("reports").data
      .toDF("allelic_composition",
            "allele_symbol",
            "genetic_background",
            "mp_id",
            "pmid",
            "mouse_gene_ids")
      .drop("allele_symbol")
    val categoriesRawDF = inputDataFrame("categories").data
      .select(
        col("mp") as "category_mp_identifier",
        col("category") as "category_mp_label"
      )
    val geneToEnsembl = inputDataFrame("targets").data
      .select(col("id"),
              flatten(array(array(col("approvedSymbol")), col("synonyms.label"))) as "gene")
      .withColumn("gene", explode(col("gene")))
      .withColumn("geneU", upper(col("gene")))
      .distinct

    // prepare reports
    val orthologyDF = orthologyRawDF
      .filter(col("phenotypes_raw").isNotNull)
      .withColumn("phenotypes_summary", split(col("phenotypes_raw"), ","))
      .withColumn("phenotypes_summary",
                  transform(col("phenotypes_summary"), (a: Column) => trim(a)))
      .drop("phenotypes_raw")
      .groupBy(
        "gene_symbol",
        "gene_id",
        "phenotypes_summary"
      )
      .agg(collect_set(col("human_gene_symbol")) as "human_genes")

    // prepare orthology
    val reportsDF = reportsRawDF
      .withColumn("mp_id", translate(col("mp_id"), ":", "_"))
      .withColumn("gene_id", explode(split(col("mouse_gene_ids"), "\\|")))
      .drop("mouse_gene_ids")

    // prepare mp
    val mousePhenotypesDF = mousePhenotypesRawDF
      .withColumn("mp_id", regexp_extract(col("id"), "MP_\\d+", 0))
      .drop("id", "path")

    // gene_id, mp_id, allelic_composition, genetic_background, pmid, label, path_codes, gene_symbol,
    // phenotypes_summary, human_genes
    val mpAndHumanGeneDf =
      reportsDF.join(mousePhenotypesDF, Seq("mp_id")).join(orthologyDF, Seq("gene_id"))

    // gene_id, path_codes, gene_symbol, phenotypes_summary, human_genes, genetype_phenotype
    val mpWithNestedGenotypePhenotypeDF = mpAndHumanGeneDf
      .withColumn(
        "genotype_phenotype",
        struct(
          col("allelic_composition") as "subject_allelic_composition",
          col("genetic_background") as "subject_background",
          col("pmid"),
          col("mp_id") as "mp_identifier",
          col("label") as "mp_label"
        )
      )
      .drop("allelic_composition", "genetic_background", "pmid", "mp_id", "label")

    // gene_id, gene_symbol, human_genes, genotype_phenotype, category_mp_identifier
    val withCategoryMpIdentifierDF = mpWithNestedGenotypePhenotypeDF
      .withColumn(
        "category_mp_identifier",
        array_distinct(transform(col("path_codes"), (a: Column) => translate(a(0), "_", ":"))))
      .withColumn("category_mp_identifier", explode(col("category_mp_identifier")))
      .drop("path_codes", "phenotypes_summary")

    // gene_id, gene_symbol, human_genes, genotype_phenotype, category_mp_identifier, category_mp_label
    val withCategoryMpLabelDF =
      withCategoryMpIdentifierDF.join(broadcast(categoriesRawDF), "category_mp_identifier")

    // "gene_id", "gene_symbol", "human_genes", "category_mp_identifier", "category_mp_label",
    // "genotype_phenotype"
    val groupedByGenotypePhenotypeDF = withCategoryMpLabelDF
      .groupBy("gene_id",
               "gene_symbol",
               "human_genes",
               "category_mp_identifier",
               "category_mp_label")
      .agg(collect_list("genotype_phenotype") as "genotype_phenotype")

    // "human_genes", "mouse_gene_id", "mouse_gene_symbol", "phenotypes"
    val groupedByHumanAndMouseGene = groupedByGenotypePhenotypeDF
      .select(
        col("human_genes"),
        col("gene_id") as "mouse_gene_id",
        col("gene_symbol") as "mouse_gene_symbol",
        struct(col("category_mp_identifier"), col("category_mp_label"), col("genotype_phenotype")) as "phenotypes"
      )
      .groupBy("human_genes", "mouse_gene_id", "mouse_gene_symbol")
      .agg(collect_set("phenotypes") as "phenotypes")

    // id, phenotypes
    groupedByHumanAndMouseGene
      .join(broadcast(geneToEnsembl),
            array_contains(col("human_genes"), col("gene")) || col("gene") === upper(
              col("mouse_gene_symbol")),
            "left_outer")
      .select(col("id"),
              struct(
                col("mouse_gene_id"),
                col("mouse_gene_symbol"),
                col("phenotypes")
              ) as "phenotypes")
      .groupBy("id")
      .agg(collect_list("phenotypes") as "phenotypes")
  }

}
