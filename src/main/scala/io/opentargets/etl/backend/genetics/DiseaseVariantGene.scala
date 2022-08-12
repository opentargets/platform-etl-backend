package io.opentargets.etl.backend.genetics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DiseaseVariantGene extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {

    val stepName = "disease-variant-gene"
    logger.info(s"Executing $stepName step.")
    implicit val ss: SparkSession = context.sparkSession

    val configuration = context.configuration.diseaseVariantGene

    logger.info(s"Configuration for $stepName: $configuration")

    logger.info(s"Loading $stepName inputs.")
    val mappedInputs = Map(
      "variant-gene" -> configuration.inputs.variantGene,
      "variant-disease" -> configuration.inputs.variantDisease
    )
    val inputs = IoHelpers.readFrom(mappedInputs)

    val vgRawDf: DataFrame = inputs("variant-gene").data
    val vdRawDf: DataFrame = inputs("variant-disease").data

    // v2d also contains rows with both overall_r2 and posterior_prob as null and
    // we don't want those to be included
    val diseaseVariantGeneDf: DataFrame = vdRawDf
      .filter(col("overall_r2").isNotNull or col("posterior_prob").isNotNull)
      .join(
        vgRawDf,
        col("chr_id") === col("tag_chrom") and
          (col("position") === col("tag_pos")) and
          (col("ref_allele") === col("tag_ref")) and
          (col("alt_allele") === col("tag_alt"))
      )
      .drop("chr_id", "position", "ref_allele", "alt_allele")
      .select(
        col("lead_chrom"), // position and chromosome values needed for ranger filtering in API.
        col("lead_pos"),
        col("tag_pos"),
        col("gene_id"),
        col("study_id"),
        col("overall_r2"),
        col("log10_ABF"),
        col("posterior_prob"),
        col("pval"),
        col("pval_exponent"),
        col("pval_mantissa"),
        col("odds_ratio"),
        col("oddsr_ci_lower"),
        col("oddsr_ci_upper"),
        col("direction"),
        col("beta"),
        col("beta_ci_lower"),
        col("beta_ci_upper"),
        concat_ws(
          "_",
          col("lead_chrom"),
          col("lead_pos"),
          col("lead_ref"),
          col("lead_alt")
        ) as "lead_variant",
        concat_ws(
          "_",
          col("tag_chrom"),
          col("tag_pos"),
          col("tag_ref"),
          col("tag_alt")
        ) as "tag_variant_id"
      )
    // write outputs
    val outputs = Map(
      "variantDiseaseGene" -> IOResource(
        diseaseVariantGeneDf,
        configuration.outputs.diseaseVariantGene
      )
    )
    logger.info(s"Write $stepName outputs.")
    IoHelpers.writeTo(outputs)
  }
}
