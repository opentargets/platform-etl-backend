package io.opentargets.etl.backend.genetics

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object VariantDisease extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {

    logger.info("Executing Variant-Disease step.")
    implicit val ss: SparkSession = context.sparkSession

    val configuration = context.configuration.variantDisease

    logger.info(s"Configuration for Variant Disease: $configuration")

    logger.info("Loading VariantDisease inputs.")
    val mappedInputs = Map(
      "variants" -> configuration.inputs.variants,
      "studies" -> configuration.inputs.studies,
      "toploci" -> configuration.inputs.toploci,
      "finemapping" -> configuration.inputs.finemapping,
      "ld" -> configuration.inputs.ld,
      "efos" -> configuration.inputs.efos
    )
    val inputs = IoHelpers.readFrom(mappedInputs)

    val variantRawDf: DataFrame = inputs("variants").data
    val efoRawDf: DataFrame = inputs("efos").data
    val studiesRawDf: DataFrame = inputs("studies").data
    val topLociRawDf: DataFrame = inputs("toploci").data

    val joinCols = Seq(
      "study_id",
      "lead_chrom",
      "lead_pos",
      "lead_ref",
      "lead_alt",
      "tag_chrom",
      "tag_pos",
      "tag_ref",
      "tag_alt"
    )
    // Prepare disease annotations: studies, top loci, locus disequilibrium, finemapping
    logger.info("Preparing variant-disease annotations")
    val studiesDf = processStudies(studiesRawDf, efoRawDf)
    val topLociDf = processTopLoci(topLociRawDf)
    val ldDf = inputs("ld").data
      .select(
        (joinCols ++ Seq(
          "overall_r2",
          "AFR_1000G_prop",
          "AMR_1000G_prop",
          "EAS_1000G_prop",
          "EUR_1000G_prop",
          "SAS_1000G_prop"
        )).map(col): _*
      )
    val fmDf = inputs("finemapping").data

    // join disease annotations into single DF to annotate variant
    val topLociAndStudiesDf = topLociDf.join(studiesDf, "study_id")
    val ldAndFmDf = ldDf.join(fmDf, joinCols, "full_outer")
    val diseaseAnnotationsDf = topLociAndStudiesDf.join(
      ldAndFmDf,
      Seq("study_id", "lead_chrom", "lead_pos", "lead_ref", "lead_alt")
    )

    // Annotate variant with disease information
    logger.info("Adding disease annotations to variant")
    val vdDf = diseaseAnnotationsDf
      .join(
        variantRawDf.select("chr_id", "position", "ref_allele", "alt_allele"),
        col("chr_id") === col("tag_chrom") and
          (col("position") === col("tag_pos") and
            (col("ref_allele") === col("tag_ref") and
              (col("alt_allele") === col("tag_alt")))),
        "inner"
      )
      .drop("chr_id", "position", "ref_allele", "alt_allele")
    // write outputs
    val outputs = Map(
      "variantDisease" -> IOResource(vdDf, configuration.outputs.variantDisease)
    )
    logger.info("Write variant-disease index outputs.")
    IoHelpers.writeTo(outputs)
  }

  private def processTopLoci(topLoci: DataFrame)(implicit ss: SparkSession): DataFrame = {
    topLoci
      .withColumn(
        "pval",
        (col("pval_mantissa") * pow(lit(10), col("pval_exponent"))).cast(DoubleType)
      )
      .withColumnRenamed("chrom", "lead_chrom")
      .withColumnRenamed("pos", "lead_pos")
      .withColumnRenamed("ref", "lead_ref")
      .withColumnRenamed("alt", "lead_alt")
  }

  private def processStudies(studies: DataFrame, efo: DataFrame)(implicit
      ss: SparkSession
  ): DataFrame = {
    val efoDf = efo
      .select(
        col("study_id"),
        coalesce(col("trait_efos"), typedLit(Array.empty)) as "trait_efos",
        coalesce(col("trait_category"), lit("Uncategorised")) as "trait_category"
      )
      .orderBy("study_id")
    val studiesDf = studies
      .select(
        col("study_id"),
        col("has_sumstats"),
        col("n_initial"),
        col("n_replication"),
        col("n_cases"),
        col("num_assoc_loci"),
        col("pmid"),
        col("pub_date"),
        col("pub_journal"),
        col("pub_title"),
        col("pub_author"),
        col("trait_reported"),
        filter(
          col("ancestry_replication"),
          c => c.isNotNull || length(c) > 0
        ) as "ancestry_replication",
        filter(
          col("ancestry_initial"),
          c => c.isNotNull || length(c) > 0
        ) as "ancestry_initial",
        regexp_extract(col("study_id"), "^([a-zA-Z]+)(.*)", 1) as "source"
      )
      .filter(col("trait_reported").isNotNull)

    studiesDf.join(efoDf, Seq("study_id"), "left_outer")
  }
}
