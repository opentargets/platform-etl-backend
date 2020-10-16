package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.{Helpers => H}

object Evidence extends LazyLogging {
  val normaliseString: String => String = c => c.replaceAll("[ .-]", "_")
  val toCamelCase: String => String = c => {
    val tokens = c
      .split("_")
      .filterNot(_.isEmpty)
    (tokens.head +: tokens.tail.map(_.capitalize)).mkString
  }

  val normAndCCase = toCamelCase compose normaliseString

  def evidenceOper(df: DataFrame): DataFrame = {

    /** apply to colName fn() and if fixedName is None camelCase and unify dots to '_' and split by it,
      * otherwise uses that fixedName */
    def trans(colName: Column,
              fixedName: Option[String] = None,
              fn: Column => Column = identity): (String, Column) = {
      val newColName = normAndCCase(
        colName
          .toString())

      fixedName.getOrElse(newColName) -> fn(colName)
    }

    val transformations = Map(
      trans(col("sourceID"), fixedName = Some("sourceId")),
      trans(col("type"), fixedName = Some("datatype_id")),
      trans(col("sourceID"), fixedName = Some("datasource_id")),
      trans(col("type"), fixedName = Some("datatype_id")),
      trans(col("disease.id")),
      trans(coalesce(col("disease.source_name"), col("disease.reported_trait")),
            fixedName = Some("diseaseFromOriginal")),
      trans(col("target.id")),
      trans(col("accession"), fn = H.stripIDFromURI),
      trans(col("drug.id"), fn = H.stripIDFromURI),
      trans(col("scores.association_score"), fixedName = Some("score")),
      trans(col("id")),
      trans(col("variant.id")),
      trans(col("variant.rs_id")),
      trans(col("evidence.allelic_requirement")),
      trans(col("evidence.biological_model.allelic_composition")),
      trans(col("evidence.biological_model.genetic_background")),
      trans(col("evidence.clinical_significance")),
      trans(col("evidence.cohort.cohort_description")),
      trans(col("evidence.cohort.cohort_id")),
      trans(col("evidence.cohort.cohort_short_name")),
      trans(col("evidence.comparison_name")),
      trans(col("evidence.confidence")),
      trans(
        col("evidence.disease_model_association.human_phenotypes"),
        fn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))),
      trans(
        col("evidence.disease_model_association.model_phenotypes"),
        fn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))),
      trans(col("evidence.drug2clinic.clinical_trial_phase.numeric_index")),
      trans(col("evidence.drug2clinic.status")),
      trans(col("evidence.drug2clinic.urls")),
      trans(col("evidence.experiment_overview")),
      trans(col("evidence.gene2variant.functional_consequence")),
      trans(col("evidence.gene2variant.provenance_type.literature.references")),
      trans(col("evidence.known_mutations")),
      trans(col("evidence.literature_ref.lit_id")),
      trans(col("evidence.literature_ref.mined_sentences")),
      trans(col("evidence.log2_fold_change.value")),
      trans(col("evidence.log2_fold_change.percentile_rank")),
      trans(col("evidence.resource_score.method.description")),
      trans(col("evidence.resource_score.method.reference")),
      trans(col("evidence.resource_score.method.url")),
      trans(col("evidence.resource_score.type")),
      trans(col("evidence.resource_score.value")),
      trans(col("evidence.significant_driver_methods")),
      trans(col("evidence.target2drug.action_type")),
      trans(col("evidence.target2drug.mechanism_of_action")),
      trans(col("evidence.target2drug.provenance_type.literature.references"),
            fn = co => transform(co, c => c.getField("lit_id"))),
      trans(col("evidence.target2drug.urls")),
      trans(col("evidence.urls")),
      trans(col("evidence.variant2disease.cases")),
      trans(col("evidence.variant2disease.confidence_interval")),
      trans(col("evidence.variant2disease.gwas_sample_size")),
      trans(col("evidence.variant2disease.odds_ratio")),
      trans(col("evidence.variant2disease.provenance_type.literature.references")),
      trans(col("evidence.variant2disease.reported_trait")),
      trans(col("evidence.variant2disease.resource_score.exponent")),
      trans(col("evidence.variant2disease.resource_score.mantissa")),
      trans(col("evidence.variant2disease.resource_score.value")),
      trans(col("evidence.variant2disease.study_link")),
      trans(col("evidence.variant2disease.urls"))
    )

    val tdf = transformations.foldLeft(df) {
      case (z, (name, oper)) => z.withColumn(name, oper)
    }

    tdf.selectExpr(transformations.keys.toSeq: _*)
  }

  def compute()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession

    val commonSec = context.configuration.common

    val mappedInputs = Map(
      "evidences" -> H.IOResourceConfig(
        commonSec.inputs.evidence.format,
        commonSec.inputs.evidence.path
      )
    )
    val dfs = H.readFrom(mappedInputs)
    val transformedDF = dfs("evidences").transform(evidenceOper)

    val newDF =
      ss.createDataFrame(transformedDF.rdd, H.renameAllCols(transformedDF.schema, normAndCCase))

    Map("processedEvidences" -> newDF)
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    val commonSec = context.configuration.common

    val processedEvidences = compute()

    val outputs = processedEvidences.keys map (name =>
      name -> H.IOResourceConfig(commonSec.outputFormat,
                                 commonSec.output + s"/$name",
                                 partitionBy = Seq("sourceId")))

    H.writeTo(outputs.toMap, processedEvidences)
  }
}
