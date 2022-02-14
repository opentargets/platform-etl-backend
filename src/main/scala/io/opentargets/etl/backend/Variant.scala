package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{abs, col, min, udaf, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/** UDAF to find the ENSG ID closest to a variant by distance. First argument should be an ENSG ID, the second is the
  * distance.
  * */
object ClosestGeneId extends Aggregator[(String, Long), (String, Long), String] {
  type GeneAndDistance = (String, Long)

  def zero: GeneAndDistance = ("", Long.MaxValue)

  def reduce(buffer: GeneAndDistance, nxt: GeneAndDistance): GeneAndDistance = {
    if (buffer._2 < nxt._2) buffer else nxt
  }

  def merge(b1: GeneAndDistance, b2: GeneAndDistance): GeneAndDistance = reduce(b1, b2)

  def finish(reduction: GeneAndDistance): String = reduction._1

  def outputEncoder: Encoder[String] = Encoders.STRING

  def bufferEncoder: Encoder[(String, Long)] = Encoders.product
}

object Variant extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {

    logger.info("Executing Variant step.")
    implicit val ss: SparkSession = context.sparkSession

    val closestGeneId = udaf(ClosestGeneId)
    ss.udf.register("closest_gene", closestGeneId)

    val variantConfiguration = context.configuration.variant

    logger.info(s"Configuration for Variant: $variantConfiguration")

    val mappedInputs = Map(
      "variants" -> variantConfiguration.inputs.variantAnnotation,
      "targets" -> variantConfiguration.inputs.targetIndex
    )
    val inputs = IoHelpers.readFrom(mappedInputs)

    val variantRawDf: DataFrame = inputs("variants").data
    val targetRawDf: DataFrame = inputs("targets").data

    val approvedBioTypes = variantConfiguration.excludedBiotypes.toSet
    val excludedChromosomes: Set[String] = Set("MT")
    logger.info("Generate target DF for variant index.")
    val targetDf = targetRawDf
      .select(
        col("id") as "gene_id",
        col("genomicLocation.*"),
        col("biotype"),
        when(col("genomicLocation.strand") > 0, col("genomicLocation.start"))
          .otherwise("genomicLocation.end") as "tss"
      )
      .filter(
        (col("biotype") isInCollection approvedBioTypes) && !(col("chromosome") isInCollection excludedChromosomes))

    logger.info("Generate protein coding DF for variant index.")
    val proteinCodingDf = targetDf.filter(col("biotype") === "protein_coding")

    logger.info("Generate variant DF for variant index.")
    val variantDf = variantRawDf
      .filter(col("chrom_b38").isNotNull && col("pos_b38").isNotNull)
      .select(
        col("chrom_b37") as "chr_id_b37",
        col("pos_b37") as "position_b37",
        col("chrom_b38") as "chr_id",
        col("pos_b38") as "position",
        col("ref") as "ref_allele",
        col("alt") as "alt_allele",
        col("rsid") as "rs_id",
        col("vep.most_severe_consequence") as "most_severe_consequence",
        col("cadd") as "cadd",
        col("af") as "af",
      )

    def variantGeneDistance(target: DataFrame) =
      variantDf
        .join(
          target,
          (col("chr_id") === col("chromosome")) && (abs(col("position") - col("tss")) <= variantConfiguration.tssDistance))
        .withColumn("d", abs(col("position") - col("tss")))

    logger.info("Calculate distance score for variant to gene.")
    val variantGeneDistanceDf = variantGeneDistance(targetDf)
    val variantPcDistanceDf = variantGeneDistance(proteinCodingDf)

    // these four components uniquely identify a variant
    val variantId = Seq("chr_id", "position", "ref_allele", "alt_allele")

    logger.info("Rank variant scores by distance")
    val variantGeneScored = variantGeneDistanceDf
      .groupBy(col("chr_id"), col("position"), col("ref_allele"), col("alt_allele"))
      .agg(closestGeneId(col("gene_id"), col("d")) as "gene_id_any",
           min(col("d")) cast LongType as "gene_id_any_distance")

    val variantPcScored = variantPcDistanceDf
      .groupBy(
        col("chr_id"),
        col("position"),
        col("ref_allele"),
        col("alt_allele"),
      )
      .agg(closestGeneId(col("gene_id"), col("d")) as "gene_id_prot_coding",
           min(col("d")) cast LongType as "gene_id_prot_coding_distance")

    logger.info("Join scored distances variants and scored protein coding.")
    val vgDistances = variantGeneScored.join(variantPcScored, variantId, "full_outer")

    logger.info("Join distances to variants.")
    val variantIndex = variantDf.join(vgDistances, variantId, "left_outer")

    val outputs = Map(
      "variant" -> IOResource(variantIndex, variantConfiguration.outputs.variants)
    )
    logger.info("Write variant index outputs.")
    IoHelpers.writeTo(outputs)
  }
}
