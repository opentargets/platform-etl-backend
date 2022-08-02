package io.opentargets.etl.backend.genetics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object VariantDiseaseColoc extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {
    val stepName = "variant-disease-coloc"
    logger.info(s"Executing $stepName step.")
    implicit val ss: SparkSession = context.sparkSession

    val configuration = context.configuration.variantDiseaseColoc

    logger.info(s"Configuration for $stepName: $configuration")

    logger.info(s"Loading $stepName inputs.")
    val mappedInputs = Map(
      "variants" -> configuration.inputs.variants,
      "targets" -> configuration.inputs.targets,
      "colocs" -> configuration.inputs.colocs
    )
    val inputs = IoHelpers.readFrom(mappedInputs)

    val variantRawDf: DataFrame = inputs("variants").data
    val targetRawDf: DataFrame = inputs("targets").data
    val colocRawDf: DataFrame = inputs("colocs").data

    val variantDf = variantRawDf.select("chr_id", "position", "ref_allele", "alt_allele")
    val targetDf: DataFrame =
      targetRawDf.select(col("id") as "gene_id", col("genomicLocation.chromosome") as "chr")
    val geneColocDf: DataFrame = colocRawDf
      .join(
        broadcast(targetDf),
        col("left_chrom") === col("chr") and col("right_gene_id") === "gene_id"
      )
      .filter(!isnan(col("coloc_h3")))
    // todo: there is a seemingly pointless filter in the original regarding right_gene_id, confirm that it doesn't do anything.

    val variantGeneColocDf: DataFrame = geneColocDf
      .join(
        variantDf,
        col("right_chrom") === col("chr_id") and
          col("right_pos") === col("position") and
          col("right_ref") === col("ref_allele") and
          col("right_alt") === col("alt_allele")
      )
      .drop(
        "chr_id",
        "position",
        "ref_allele",
        "alt_allele",
        "chr",
        "gene_id"
      )

    // write outputs
    val outputs = Map(
      "variantDiseaseColoc" -> IOResource(
        variantGeneColocDf,
        configuration.outputs.variantDiseaseColoc
      )
    )
    logger.info(s"Write $stepName outputs.")
    IoHelpers.writeTo(outputs)
  }

}
