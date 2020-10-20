package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.{Helpers => H}

object Evidence extends LazyLogging {
  val directStringMapping: (String, Map[String, String]) => String = (n, m) =>
    m.withDefaultValue(n)(n)
  val normaliseString: String => String = c => c.replaceAll("[ .-]", "_")
  val toCamelCase: String => String = c => {
    val tokens = c
      .split("_")
      .filterNot(_.isEmpty)
    (tokens.head +: tokens.tail.map(_.capitalize)).mkString
  }

  val normAndCCase = toCamelCase compose normaliseString

  /** apply to colName fn() and if fixedName is None camelCase and unify dots to '_' and split by it,
    * otherwise uses that fixedName */
  def trans(inColumn: Column,
            newNameFn: String => String,
            columnFn: Column => Column): (String, Column) = {

    newNameFn(inColumn.toString) -> columnFn(inColumn)
  }

  val flattenC = trans(_, normAndCCase, identity)
  val flattenCAndSetN = trans(_, _, identity)
  val flattenCAndSetC = trans(_, normAndCCase, _)

  def evidenceOper(df: DataFrame): DataFrame = {
    val transformations = Map(
      flattenCAndSetN(col("sourceID"), _ => "sourceId"),
      flattenCAndSetN(col("type"), _ => "datatype_id"),
      flattenCAndSetN(col("sourceID"), _ => "datasource_id"),
      flattenCAndSetN(col("type"), _ => "datatype_id"),
      flattenC(col("disease.id")),
      flattenCAndSetN(coalesce(col("disease.source_name"), col("disease.reported_trait")),
                      _ => "diseaseFromSource"),
      flattenC(col("target.id")),
      flattenCAndSetC(col("drug.id"), H.stripIDFromURI),
      flattenCAndSetN(col("scores.association_score"), _ => "score"),
      flattenC(col("id")),
      flattenC(col("variant.id")),
      flattenC(col("variant.rs_id")),
      flattenC(col("evidence.allelic_requirement")),
      flattenC(col("evidence.biological_model.allelic_composition")),
      flattenC(col("evidence.biological_model.genetic_background")),
      flattenCAndSetN(coalesce(col("evidence.clinical_significance"),
                               col("evidence.variant2disease.clinical_significance")),
                      _ => "evidenceClinicalSignificance"),
      flattenCAndSetN(col("evidence.cohort.cohort_description"),
                      n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.cohort.cohort_id"),
                      n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.cohort.cohort_short_name"),
                      n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.comparison_name"), _ => "evidenceContrast"),
      flattenC(col("evidence.confidence")),
      trans(
        col("evidence.disease_model_association.human_phenotypes"),
        newNameFn = _ => "evidenceDiseaseModelAssociatedHumanPhenotypes",
        columnFn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))
      ),
      trans(
        col("evidence.disease_model_association.model_phenotypes"),
        newNameFn = _ => "evidenceDiseaseModelAssociatedModelPhenotypes",
        columnFn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))
      ),
      flattenCAndSetN(col("evidence.drug2clinic.clinical_trial_phase.numeric_index"),
                      _ => "evidenceClinicalPhase"),
      flattenCAndSetN(col("evidence.drug2clinic.status"), _ => "evidenceClinicalStatus"),
      flattenCAndSetN(col("evidence.drug2clinic.urls"), _ => "evidenceClinicalUrls"),
      flattenC(col("evidence.experiment_overview")),
      flattenCAndSetN(col("evidence.gene2variant.functional_consequence"),
                      _ => "evidenceVariantFunctionalConsequenceUri"),
      flattenCAndSetN(
        coalesce(col("evidence.gene2variant.provenance_type.literature.references"),
                 col("evidence.variant2disease.provenance_type.literature.references")),
        _ => "evidenceLiteratureReference"
      ),
      flattenCAndSetN(col("evidence.known_mutations"), _ => "evidenceVariations"),
      flattenC(col("evidence.literature_ref.lit_id")),
      flattenCAndSetN(col("evidence.literature_ref.mined_sentences"),
                      _ => "evidenceTextMiningSentences"),
      flattenC(col("evidence.log2_fold_change.value")),
      flattenC(col("evidence.log2_fold_change.percentile_rank")),
      flattenC(col("evidence.resource_score.type")),
      flattenCAndSetN(coalesce(col("evidence.resource_score.value"),
                               col("evidence.variant2disease.resource_score.value")),
                      _ => "evidenceResourceScore"),
      flattenC(col("evidence.significant_driver_methods")),
      flattenCAndSetN(col("evidence.target2drug.action_type"), _ => "evidenceDrugActionType"),
      flattenCAndSetN(col("evidence.target2drug.mechanism_of_action"),
                      _ => "evidenceDrugMechanismOfAction"),
      flattenCAndSetC(col("evidence.target2drug.provenance_type.literature.references"),
                      co => transform(co, c => c.getField("lit_id"))),
      flattenCAndSetN(col("evidence.target2drug.urls"), _ => "evidenceDrugMechanismOfActionUris"),
      flattenCAndSetN(coalesce(col("evidence.urls"), col("evidence.variant2disease.urls")),
                      _ => "evidenceUrls"),
      flattenC(col("evidence.variant2disease.cases")),
      flattenCAndSetN(col("evidence.variant2disease.confidence_interval"),
                      _ => "evidenceConfidenceInterval"),
      flattenC(col("evidence.variant2disease.gwas_sample_size")),
      flattenCAndSetN(col("evidence.variant2disease.odds_ratio"), _ => "evidenceOddsRatio"),
      flattenCAndSetN(col("evidence.variant2disease.resource_score.exponent"),
                      _ => "evidenceResourceScoreExponent"),
      flattenC(col("evidence.variant2disease.resource_score.mantissa")),
      trans(col("evidence.variant2disease.study_link"), _ => "evidenceStudyId", H.stripIDFromURI),
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

    val namesMap =
      Map("author" -> "firstAuthor",
          "year" -> "publicationYear",
          "lit_id" -> "Url",
          "functional_consequence" -> "functionalConsequenceUri")

    val nestedNames = directStringMapping(_, namesMap)

    val newDF =
      ss.createDataFrame(transformedDF.rdd,
                         H.renameAllCols(transformedDF.schema, nestedNames compose normAndCCase))

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
