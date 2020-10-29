package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.{Helpers => H}

object Evidence extends LazyLogging {
  val directStringMapping: (String, Map[String, String]) => String = (n, m) =>
    m.withDefaultValue(n)(n)
  val removeEvidencePrefix: String => String = c => c.stripPrefix("evidence.")
  val normaliseString: String => String = c => c.replaceAll("[ .-]", "_")
  val toCamelCase: String => String = c => {
    val tokens = c
      .split("_")
      .filterNot(_.isEmpty)
    (tokens.head +: tokens.tail.map(_.capitalize)).mkString
  }

  // if it contains '_' then we dont want it (ex: 1_1234_A_ATT)
  val skipVariantIDs = ((cc: Column) => when(cc.isNotNull and size(split(cc, "_")) === 1, cc)) compose H.stripIDFromURI

  val normAndCCase = toCamelCase compose normaliseString compose removeEvidencePrefix

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
      flattenCAndSetN(col("target.activity"), _ => "targetModulation"),
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
                      _ => "clinicalSignificance"),
      flattenCAndSetN(col("evidence.cohort.cohort_description"),
                      n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.cohort.cohort_id"),
                      n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.cohort.cohort_short_name"),
                      n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.comparison_name"), _ => "contrast"),
      flattenC(col("evidence.confidence")),
      H.trans(
        col("evidence.disease_model_association.human_phenotypes"),
        newNameFn = _ => "diseaseModelAssociatedHumanPhenotypes",
        columnFn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))
      ),
      H.trans(
        col("evidence.disease_model_association.model_phenotypes"),
        newNameFn = _ => "diseaseModelAssociatedModelPhenotypes",
        columnFn = co =>
          transform(co, c => struct(c.getField("id").as("id"), c.getField("label").as("label")))
      ),
      flattenCAndSetN(col("evidence.drug2clinic.clinical_trial_phase.numeric_index"),
                      _ => "clinicalPhase"),
      H.trans(coalesce(col("evidence.drug2clinic.status"), lit("N/A")),
              _ => "clinicalStatus",
              co => when(col("sourceID") === "chembl", co)),
      flattenCAndSetN(col("evidence.drug2clinic.urls"), _ => "clinicalUrls"),
      flattenC(col("evidence.experiment_overview")),
      H.trans(col("evidence.gene2variant.functional_consequence"),
              _ => "variantFunctionalConsequenceId",
              H.stripIDFromURI),
      flattenCAndSetN(when(col("sourceID") === "ot_genetics_portal",
                           col("evidence.gene2variant.resource_score.value")),
                      _ => "locus2GeneScore"),
      flattenCAndSetN(
        when(col("sourceID") === "eva", col("evidence.gene2variant.resource_score.value")),
        _ => "variantFunctionalConsequenceScore"),
      H.trans(
        col("evidence.variant2disease.provenance_type.literature.references"),
        _ => "publicationFirstAuthor",
        c => when(col("sourceID") === "ot_genetics_portal", c.getItem(0).getField("author"))
      ),
      H.trans(
        col("evidence.variant2disease.provenance_type.literature.references"),
        _ => "publicationYear",
        c => when(col("sourceID") === "ot_genetics_portal", c.getItem(0).getField("year"))
      ),
      H.trans(
        col("evidence.known_mutations"),
        _ => "variations",
        c =>
          transform(
            c,
            co =>
              struct(
                H.stripIDFromURI(co.getField("functional_consequence")).as("functionalConsequence"),
                co.getField("inheritance_pattern").as("inheritancePattern"),
                co.getField("number_mutated_samples").as("numberMutatedSamples"),
                co.getField("number_samples_tested").as("numberSamplesTested"),
                co.getField("number_samples_with_mutation_type").as("numberSamplesWithMutationType"),
                when(col("sourceID") === "reactome", co.getField("preferred_name")).as("variantAminoacidDescription")
            )
        )
      ),
      flattenCAndSetN(col("evidence.literature_ref.mined_sentences"),
                      _ => "textMiningSentences"),
      flattenC(col("evidence.log2_fold_change.value")),
      flattenC(col("evidence.log2_fold_change.percentile_rank")),
      flattenC(col("evidence.resource_score.type")),
      H.trans(col("evidence.resource_score.method.description"),
              _ => "studyOverview",
              c => when(col("sourceID") === "sysbio", c)),
      flattenCAndSetN(coalesce(col("evidence.resource_score.value"),
                               col("evidence.variant2disease.resource_score.value")),
                      _ => "resourceScore"),
      flattenC(col("evidence.significant_driver_methods")),
      H.trans(
        coalesce(col("evidence.urls"), col("evidence.variant2disease.urls")),
        _ => "recordId",
        c =>
          when(col("sourceID") isInCollection List("eva", "eva_somatic", "clingen"),
               transform(c, co => H.stripIDFromURI(co.getField("url"))).getItem(0))
      ),
      H.trans(
        coalesce(col("evidence.variant2disease.urls"), col("evidence.urls")),
        _ => "pathwayId",
        c =>
          when(col("sourceID") isInCollection List("progeny", "reactome", "slapenrich"),
               transform(c, co => ltrim(H.stripIDFromURI(co.getField("url")), "#")).getItem(0))
      ),
      H.trans(
        coalesce(col("evidence.variant2disease.urls"), col("evidence.urls")),
        _ => "pathwayName",
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
        _ => "studyId",
        c =>
          when(col("sourceID") isInCollection List("expression_atlas", "ot_genetics_portal"),
               H.stripIDFromURI(c))
            .when(col("sourceID") === "genomics_england", element_at(reverse(split(c, "/")), 2))
      ),
      flattenCAndSetN(col("evidence.variant2disease.cases"), _ => "studyCases"),
      H.trans(
        col("evidence.variant2disease.confidence_interval"),
        _ => "confidenceIntervalLower",
        c => when(c.isNotNull, split(c, "-").getItem(0).cast(DoubleType))
      ),
      H.trans(
        col("evidence.variant2disease.confidence_interval"),
        _ => "confidenceIntervalUpper",
        c => when(c.isNotNull, split(c, "-").getItem(1).cast(DoubleType))
      ),
      flattenCAndSetN(col("evidence.variant2disease.gwas_sample_size"),
                      _ => "studySampleSize"),
      H.trans(col("evidence.variant2disease.odds_ratio"),
              _ => "oddsRatio",
              c => when(c.isNotNull, c.cast(DoubleType))),
      flattenCAndSetN(col("evidence.variant2disease.resource_score.exponent"),
                      _ => "resourceScoreExponent"),
      flattenCAndSetN(col("evidence.variant2disease.resource_score.mantissa"),
                      _ => "resourceScoreMantissa"),
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
                  when(
                    !col("sourceID").isInCollection(
                      List("slapenrich", "intogen", "progeny", "gene2phenotype")),
                    transform(coalesce(col("evidence.provenance_type.literature.references"),
                                       array()),
                              c => c.getField("lit_id"))
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
        _ => "literature"
      )
    )

    val tdf = transformations.foldLeft(df) {
      case (z, (name, oper)) => z.withColumn(name, oper)
    }
    // evidence literature is a bit tricky and this is a temporal thing till the providers clean all
    // the pending fields that need to be moved, renamed or removed.
    tdf
      .withColumn("literature",
                  when(size(col("literature")) > 0, col("literature")))
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
