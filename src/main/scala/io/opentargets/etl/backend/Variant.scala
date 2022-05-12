package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.genetics.Gene
import io.opentargets.etl.backend.genetics.Gene.variantGeneDistance
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions.{
  abs,
  arrays_zip,
  col,
  collect_list,
  collect_set,
  explode,
  map_from_entries,
  min,
  struct,
  when
}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Variant extends LazyLogging {

  // these four components uniquely identify a variant
  val variantIdStr: Seq[String] = Seq("chr_id", "position", "ref_allele", "alt_allele")
  val variantIdCol: Seq[Column] = variantIdStr.map(col)

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

    logger.info("Generate target DF for variant index.")
    val targetDf =
      Gene.getGeneDf(targetRawDf, context.configuration.genetics.approvedBiotypes)

    logger.info("Generate protein coding DF for variant index.")
    val proteinCodingDf = targetDf.filter(col("biotype") === "protein_coding")

    logger.info("Generate variant DF for variant index.")
    // fixme: filter by valid chromosomes to get rid of the alternative assemblies
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
        col("vep.transcript_consequences") as "vep",
        col("cadd") as "cadd",
        col("af") as "af"
      )
      .repartition(variantIdCol: _*)

    val vep = variantDf
      .select(
        variantIdCol :+ explode(col("vep")).as("vep"): _*
      )
      .select(
        variantIdCol ++ Seq(
          col("vep.gene_id") as "vep_gene_id",
          col("vep.consequence_terms").getItem(0) as "vep_consequence"
        ): _*
      )
      .groupBy(variantIdCol :+ col("vep_gene_id"): _*)
      .agg(collect_set("vep_consequence") as "fpred_labels")
      .groupBy(variantIdCol: _*)
      .agg(collect_list(struct("vep_gene_id", "fpred_labels")) as "vep")

    val variantWithVep = variantDf.drop("vep").join(vep, variantIdStr, "left_outer").cache

    logger.info("Calculate distance score for variant to gene.")
    val variantGeneDistanceDf =
      targetDf.transform(variantGeneDistance(variantWithVep, variantConfiguration.tssDistance))
    val variantPcDistanceDf = proteinCodingDf.transform(
      variantGeneDistance(variantWithVep, variantConfiguration.tssDistance)
    )

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

    // fixme: I don't think that these fields are used. This is just the variant index, the variant-gene
    // index should include this information if it is relevant. Even it if is not, we can get the
    // information by looking at the top ranked distance gene in the variant-gene index.
    val variantGeneScored = variantGeneDistanceDf.transform(findNearestGene("gene_id_any"))
    val variantPcScored = variantPcDistanceDf.transform(findNearestGene("gene_id_prot_coding"))

    logger.info("Join scored distances variants and scored protein coding.")
    val vgDistances = variantGeneScored.join(variantPcScored, variantIdStr, "full_outer")

    logger.info("Join distances to variants.")
    val variantIndex = variantWithVep.join(vgDistances, variantIdStr, "left_outer")

    val outputs = Map(
      "variant" -> IOResource(variantIndex, variantConfiguration.outputs.variants)
    )
    logger.info("Write variant index outputs.")
    // fixme: save output partitioned by chromosome
    IoHelpers.writeTo(outputs)
  }
}
