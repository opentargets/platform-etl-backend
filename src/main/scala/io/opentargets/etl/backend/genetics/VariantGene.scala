package io.opentargets.etl.backend.genetics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.functions.{
  array_max,
  col,
  collect_set,
  element_at,
  explode,
  lit,
  map_filter,
  map_from_entries,
  map_keys,
  regexp_extract,
  split,
  struct
}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.:+

object VariantGene extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {

    logger.info("Executing Variant-Gene step.")
    implicit val ss: SparkSession = context.sparkSession

    val configuration = context.configuration.variantGene

    logger.info(s"Configuration for Variant: $configuration")

    val mappedInputs = Map(
      "variants" -> configuration.inputs.variantIndex,
      "targets" -> configuration.inputs.targetIndex,
      "qtl" -> configuration.inputs.qtl,
      "vep" -> configuration.inputs.vepConsequences,
      "interval" -> configuration.inputs.interval
    )
    val inputs = IoHelpers.readFrom(mappedInputs)

    val variantRawDf: DataFrame = inputs("variants").data
    val targetRawDf: DataFrame = inputs("targets").data
    val qtlRawDf: DataFrame = inputs("qtl").data
    val vepRawDf: DataFrame = inputs("vep").data
    val intervalRawDf: DataFrame = inputs("interval").data

    val idxCols = Seq("chr_id", "position", "ref_allele", "alt_allele").map(col)
    val groupingCols = idxCols :+ col("vep_gene_id")

    val variantIdx: DataFrame = variantRawDf
      .select(
        col("chr_id"),
        col("position"),
        col("ref_allele"),
        col("alt_allele"),
        explode(col("vep")) as "vep"
      )
      .select(
        idxCols :+ col("vep.*"): _*
      )

    // veps
    val vepConsequencesDf = vepRawDf
      .filter(col("v2g_score").isNotNull)
      .select(
        col("Term") as "fpred_label",
        col("v2g_score") as "fpred_score"
      )

    val variantGeneVepDf = variantIdx
      .select(groupingCols :+ (explode(col("fpred_labels")) as "fpred_label"): _*)
      .join(vepConsequencesDf, Seq("fpred_label"), "left_outer")
      .groupBy(groupingCols: _*)
      .agg(collect_set(struct(col("fpred_score"), col("fpred_label"))) as "f")
      .select(
        idxCols ++ Seq(
          map_filter(map_from_entries(col("f")), (k, _) => k > 0F).as("score_label_map"),
          col("f.fpred_score") as "fpred_scores",
          col("f.fpred_label") as "fpred_labels",
          lit("fpred").as("type_id"),
          lit("vep").as("source_id"),
          lit("unspecified").as("feature"),
        ): _*
      )
      .withColumn("fpred_max_score", array_max(map_keys(col("score_label_map"))))
      .withColumn("fpred_max_label", element_at(col("score_label_map"), col("fpred_max_score")))
      .filter(col("fpred_max_score").isNotNull)
      .drop("score_label_map")

    val variantGeneIdx: DataFrame = ???

    val outputs = Map(
      "variantGene" -> IOResource(variantGeneIdx, configuration.outputs.variantGene)
    )
    logger.info("Write variant-gene index outputs.")
    IoHelpers.writeTo(outputs)
  }
}
