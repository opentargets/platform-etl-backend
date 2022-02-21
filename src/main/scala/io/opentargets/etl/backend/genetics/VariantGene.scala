package io.opentargets.etl.backend.genetics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.functions.{col, map_from_entries, regexp_extract, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    val variantIdx = variantRawDf
      .select(
        col("chr_id"),
        col("position"),
        col("ref_allele"),
        col("alt_allele"),
        col("gene_id"),
        col("transcript_consequences")
      )
      .filter(col("transcript_consequences").isNotNull)
    // veps
    val vepDf = vepRawDf
      .filter(col("v2g_score").isNotNull)
      .select(
        regexp_extract(col("Accession"), ".+/(.+)", 1) as "accession",
        col("Term") as "term",
        col("Description") as "description",
        col("Display term") as "display_term",
        col("IMPACT") as "impact",
        col("v2g_score"),
        col("eco_score"),
      )

    val variantGeneIdx: DataFrame = ???
    val outputs = Map(
      "variantGene" -> IOResource(variantGeneIdx, configuration.outputs.variantGene)
    )
    logger.info("Write variant-gene index outputs.")
    IoHelpers.writeTo(outputs)
  }
}
