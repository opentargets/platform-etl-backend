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

  // if it contains '_' then we dont want it (ex: 1_1234_A_ATT)
  val skipVariantIDs = ((cc: Column) => when(cc.isNotNull and size(split(cc, "_")) === 1, cc)) compose H.stripIDFromURI

  val normAndCCase = toCamelCase compose normaliseString

  val flattenC = H.trans(_, normAndCCase, identity)
  val flattenCAndSetN = H.trans(_, _, identity)
  val flattenCAndSetC = H.trans(_, normAndCCase, _)

  def evidenceOper(df: DataFrame): DataFrame = {
    val transformations = Map(
      flattenCAndSetN(col("sourceID"), _ => "sourceId"),
      flattenCAndSetN(col("sourceID"), _ => "datasourceId"),
      flattenCAndSetN(col("type"), _ => "datatypeId"),
      flattenC(col("disease.id")),
      flattenCAndSetN(coalesce(col("disease.source_name"), col("disease.reported_trait")),
                      _ => "diseaseFromSource"),
      flattenC(col("target.id")),
      flattenCAndSetC(col("drug.id"), H.stripIDFromURI),
      flattenCAndSetN(col("scores.association_score"), _ => "score"),
      flattenC(col("id")),
      flattenCAndSetC(col("variant.id"),
                      c => when(col("sourceID") === "ot_genetics_portal", H.stripIDFromURI(c))),
      H.trans(coalesce(col("variant.rs_id"), col("variant.id")),
              _ => "variantRsId",
              skipVariantIDs),
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
      H.trans(
        col("evidence.disease_model_association.human_phenotypes"),
        newNameFn = _ => "evidenceDiseaseModelAssociatedHumanPhenotypes",
        columnFn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))
      ),
      H.trans(
        col("evidence.disease_model_association.model_phenotypes"),
        newNameFn = _ => "evidenceDiseaseModelAssociatedModelPhenotypes",
        columnFn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))
      ),
      flattenCAndSetN(col("evidence.drug2clinic.clinical_trial_phase.numeric_index"),
                      _ => "evidenceClinicalPhase"),
      H.trans(coalesce(col("evidence.drug2clinic.status"), lit("N/A")),
              _ => "evidenceClinicalStatus",
              co => when(col("sourceID") === "chembl", co)),
      flattenCAndSetN(col("evidence.drug2clinic.urls"), _ => "evidenceClinicalUrls"),
      flattenC(col("evidence.experiment_overview")),
      H.trans(col("evidence.gene2variant.functional_consequence"),
              _ => "evidenceVariantFunctionalConsequenceId",
              H.stripIDFromURI),
      flattenCAndSetN(when(col("sourceID") === "ot_genetics_portal",
                           col("evidence.gene2variant.resource_score.value")),
                      _ => "evidenceLocus2GeneScore"),
      flattenCAndSetN(
        when(col("sourceID") === "eva", col("evidence.gene2variant.resource_score.value")),
        _ => "evidenceVariantFunctionalConsequenceScore"),
      H.trans(
        col("evidence.variant2disease.provenance_type.literature.references"),
        _ => "evidencePublicationFirstAuthor",
        c => when(col("sourceID") === "ot_genetics_portal", c.getItem(0).getField("author"))
      ),
      H.trans(
        col("evidence.variant2disease.provenance_type.literature.references"),
        _ => "evidencePublicationYear",
        c => when(col("sourceID") === "ot_genetics_portal", c.getItem(0).getField("year"))
      ),
      flattenCAndSetN(col("evidence.known_mutations"), _ => "evidenceVariations"),
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
      H.trans(
        coalesce(col("evidence.urls"), col("evidence.variant2disease.urls")),
        _ => "evidenceRecordId",
        c =>
          when(col("sourceID") isInCollection List("eva", "eva_somatic", "clingen"),
               transform(c, co => H.stripIDFromURI(co.getField("url"))).getItem(0))
      ),
      H.trans(
        coalesce(col("evidence.variant2disease.urls"), col("evidence.urls")),
        _ => "evidencePathwayId",
        c =>
          when(col("sourceID") isInCollection List("progeny", "reactome", "slapenrich"),
               transform(c, co => ltrim(H.stripIDFromURI(co.getField("url")), "#")).getItem(0))
      ),
      H.trans(
        coalesce(col("evidence.variant2disease.urls"), col("evidence.urls")),
        _ => "evidencePathwayName",
        c =>
          when(col("sourceID") isInCollection List("progeny", "reactome", "slapenrich"),
               trim(transform(c, co => co.getField("nice_name")).getItem(0)))
      ),
      H.trans(
        coalesce(
          col("evidence.variant2disease.study_link"),
          col("evidence.urls").getItem(0).getField("url"),
          col("evidence.variant2disease.urls").getItem(0).getField("url")
        ),
        _ => "evidenceStudyId",
        c =>
          when(col("sourceID") isInCollection List("expression_atlas", "ot_genetics_portal"),
               H.stripIDFromURI(c))
            .when(col("sourceID") === "genomics_england", element_at(reverse(split(c, "/")), 2))
      ),
      flattenCAndSetN(col("evidence.variant2disease.cases"),
        _ => "evidenceStudyCases"),
      H.trans(
        col("evidence.variant2disease.confidence_interval"),
        _ => "evidenceConfidenceIntervalLower",
        c => when(c.isNotNull, split(c, "-").getItem(0).cast(DoubleType))
      ),
      H.trans(
        col("evidence.variant2disease.confidence_interval"),
        _ => "evidenceConfidenceIntervalUpper",
        c => when(c.isNotNull, split(c, "-").getItem(1).cast(DoubleType))
      ),
      flattenCAndSetN(col("evidence.variant2disease.gwas_sample_size"),
        _ => "evidenceStudySampleSize"),
      H.trans(col("evidence.variant2disease.odds_ratio"),
              _ => "evidenceOddsRatio",
              c => when(c.isNotNull, c.cast(DoubleType))),
      flattenCAndSetN(col("evidence.variant2disease.resource_score.exponent"),
                      _ => "evidenceResourceScoreExponent"),
      flattenCAndSetN(col("evidence.variant2disease.resource_score.mantissa"),
                      _ => "evidenceResourceScoreMantissa"),
      flattenCAndSetN(
        when(
          col("sourceID") =!= "phewas_catalog",
          array_distinct(
            transform(
              filter(
                concat(
                  when(
                    col("sourceID") isInCollection List("sysbio", "crispr"),
                    array(col("evidence.resource_score.method.reference"))
                  ).otherwise(array()),
                  transform(
                    coalesce(col("evidence.gene2variant.provenance_type.literature.references"),
                             array()),
                    c => c.getField("lit_id")),
                  transform(
                    coalesce(col("evidence.variant2disease.provenance_type.literature.references"),
                             array()),
                    c => c.getField("lit_id")),
                  when(col("evidence.literature_ref.lit_id").isNotNull,
                       array(col("evidence.literature_ref.lit_id")))
                    .otherwise(array())
                ),
                co => co.isNotNull
              ),
              cc => H.stripIDFromURI(cc)
            ))
        ).otherwise(array()),
        _ => "evidenceLiterature"
      )
    )

    val tdf = transformations.foldLeft(df) {
      case (z, (name, oper)) => z.withColumn(name, oper)
    }

    // evidence literature is a bit tricky and this is a temporal thing till the providers clean all
    // the pending fields that need to be moved, renamed or removed.
    tdf
      .withColumn("evidenceLiterature",
                  when(size(col("evidenceLiterature")) > 0, col("evidenceLiterature")))
      .selectExpr(transformations.keys.toSeq: _*)
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
      Map("functional_consequence" -> "functionalConsequenceUri")

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
