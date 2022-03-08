package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions.{
  abs,
  arrays_zip,
  col,
  collect_list,
  map_from_entries,
  min,
  udaf,
  when
}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Variant extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {

    logger.info("Executing Variant step.")
    implicit val ss: SparkSession = context.sparkSession

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

    // these four components uniquely identify a variant
    val variantIdStr = Seq("chr_id", "position", "ref_allele", "alt_allele")
    val variantIdCol = variantIdStr.map(col)

    logger.info("Generate target DF for variant index.")
    val targetDf = targetRawDf
      .select(
        col("id") as "gene_id",
        col("genomicLocation.*"),
        col("biotype"),
        when(col("genomicLocation.strand") > 0, col("genomicLocation.start"))
          .otherwise(col("genomicLocation.end")) as "tss"
      )
      .filter(
        (col("biotype") isInCollection approvedBioTypes) && !(col(
          "chromosome"
        ) isInCollection excludedChromosomes)
      )

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
        col("af") as "af"
      )
      .repartition(variantIdCol: _*)

    def variantGeneDistance(target: DataFrame): DataFrame =
      variantDf
        .join(
          target,
          (col("chr_id") === col("chromosome")) && (abs(
            col("position") - col("tss")
          ) <= variantConfiguration.tssDistance)
        )
        .withColumn("d", abs(col("position") - col("tss")))

    logger.info("Calculate distance score for variant to gene.")
    val variantGeneDistanceDf = targetDf.transform(variantGeneDistance)
    val variantPcDistanceDf = proteinCodingDf.transform(variantGeneDistance)

    logger.info("Rank variant scores by distance")

    def findNearestGene(name: String)(df: DataFrame): DataFrame = {
      val nameDistance = s"${name}_distance"
      df.groupBy(variantIdCol: _*)
        .agg(
          collect_list(col("gene_id")) as "geneList",
          collect_list(col("d")) as "dist",
          min(col("d")) cast LongType as nameDistance
        )
        .select(
          variantIdCol ++ Seq(
            col(nameDistance),
            map_from_entries(arrays_zip(col("dist"), col("geneList"))) as "distToGeneMap"
          ): _*
        )
        .withColumn(name, col("distToGeneMap")(col(nameDistance)))
        .drop("distToGeneMap", "geneList", "dist")
    }

    val variantGeneScored = variantGeneDistanceDf.transform(findNearestGene("gene_id_any"))
    val variantPcScored = variantPcDistanceDf.transform(findNearestGene("gene_id_prot_coding"))

    logger.info("Join scored distances variants and scored protein coding.")
    val vgDistances = variantGeneScored.join(variantPcScored, variantIdStr, "full_outer")

    logger.info("Join distances to variants.")
    val variantIndex = variantDf.join(vgDistances, variantIdStr, "left_outer")

    val outputs = Map(
      "variant" -> IOResource(variantIndex, variantConfiguration.outputs.variants)
    )
    logger.info("Write variant index outputs.")
    IoHelpers.writeTo(outputs)
  }
}
