package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.mkFlattenArray
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
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

  def reshape(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val transformations = Map(
      flattenCAndSetN(col("sourceID"), _ => "sourceId"),
      flattenCAndSetN(col("sourceID"), _ => "datasourceId"),
      flattenCAndSetN(col("type"), _ => "datatypeId"),
      flattenCAndSetN(col("evidence.cell_lines"), _ => "diseaseCellLines"),
      flattenCAndSetN(col("evidence.original_disease_array"), _ => "cohortPhenotypes"),
      flattenCAndSetC(col("disease.id"), H.stripIDFromURI),
      H.trans(col("unique_association_fields.model_gene_id"),
        _ => "targetInModel",
        H.stripIDFromURI),
      H.trans(col("unique_association_fields.reaction_id"), _ => "reactionId", H.stripIDFromURI),
      flattenCAndSetN(
        coalesce(
          when(col("sourceID") isInCollection List("genomics_england"),
            element_at(from_json(col("disease.source_name"), ArrayType(StringType)), 1))
            .otherwise(col("disease.source_name")),
          when(col("sourceID") isInCollection List("uniprot_literature", "europepmc", "reactome"),
            col("disease.name")),
          col("disease.reported_trait"),
          col("unique_association_fields.tumor_type")
        ),
        _ => "diseaseFromSource"
      ),

      flattenCAndSetC(col("target.id"), H.stripIDFromURI),
      H.trans(col("target.id"), _ => "targetFromSourceId", H.stripIDFromURI),
      H.trans(coalesce(col("unique_association_fields.disease_phenodigm_id"),
        col("disease.id")), _ => "diseaseFromSourceId", H.stripIDFromURI),
      flattenCAndSetC(col("drug.id"), H.stripIDFromURI),
      flattenCAndSetC(col("variant.id"), H.stripIDFromURI),
      flattenCAndSetC(col("variant.rs_id"), H.stripIDFromURI),
      H.trans(
        col("target.activity"),
        _ => "targetModulation",
        c => when(col("sourceID") === "reactome", c)
      ),
      flattenCAndSetN(
        flatten(
          array(
            coalesce(col("evidence.variant2disease.mode_of_inheritance"),
              array(col("evidence.allelic_requirement")),
              typedLit(Seq.empty[String])))),
        _ => "allelicRequirements"
      ),
      flattenC(col("evidence.biological_model.allelic_composition")),
      flattenC(col("evidence.biological_model.genetic_background")),
      flattenCAndSetN(coalesce(col("evidence.clinical_significance"),
        col("evidence.variant2disease.clinical_significance")),
        _ => "clinicalSignificances"),
      flattenCAndSetN(col("evidence.cohort.cohort_description"),
        n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.cohort.cohort_id"),
        n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.cohort.cohort_short_name"),
        n => normAndCCase(n.replaceFirst("\\.cohort", ""))),
      flattenCAndSetN(col("evidence.comparison_name"), _ => "contrast"),
      flattenCAndSetN(coalesce(col("evidence.variant2disease.clinvar_rating.review_status"),
        col("evidence.confidence")),
        _ => "confidence"),
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
      H.trans(col("evidence.gene2variant.functional_consequence"),
        _ => "variantFunctionalConsequenceId",
        H.stripIDFromURI),
      flattenCAndSetN(when(col("sourceID") === "ot_genetics_portal",
        col("evidence.gene2variant.resource_score.value")),
        _ => "locus2GeneScore"),
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
        _ => "variants",
        c =>
          transform(
            c,
            co =>
              struct(
                H.stripIDFromURI(co.getField("functional_consequence"))
                  .as("functionalConsequenceId"),
                co.getField("number_mutated_samples").as("numberMutatedSamples"),
                co.getField("number_samples_tested").as("numberSamplesTested"),
                co.getField("number_samples_with_mutation_type")
                  .as("numberSamplesWithMutationType"),
                when(col("sourceID") === "reactome", co.getField("preferred_name"))
                  .as("aminoacidDescription")
              )
          )
      ),
      flattenCAndSetN(col("evidence.literature_ref.mined_sentences"), _ => "textMiningSentences"),
      flattenC(col("evidence.log2_fold_change.value")),
      flattenC(col("evidence.log2_fold_change.percentile_rank")),
      flattenCAndSetN(
        coalesce(
          col("evidence.study_overview"),
          when(
            col("sourceID") isInCollection List("sysbio", "expression_atlas"),
            coalesce(col("evidence.experiment_overview"),
              col("evidence.resource_score.method.description"))
          )
        ),
        _ => "studyOverview"
      ),
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
          col("evidence.study_id"),
          col("evidence.variant2disease.study_link"),
          col("evidence.urls").getItem(0).getField("url"),
          col("evidence.variant2disease.urls").getItem(0).getField("url")
        ),
        _ => "studyId",
        H.stripIDFromURI
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
      flattenCAndSetN(col("evidence.variant2disease.gwas_sample_size"), _ => "studySampleSize"),
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

    val tdf = transformations
      .foldLeft(df) {
        case (z, (name, oper)) => z.withColumn(name, oper)
      }
      // evidence literature is a bit tricky and this is a temporal thing till the providers clean all
      // the pending fields that need to be moved, renamed or removed.
      .withColumn("literature", when(size(col("literature")) > 0, col("literature")))
      .withColumn("allelicRequirements",
        when(size(filter(col("allelicRequirements"), c => c.isNotNull)) > 0,
          col("allelicRequirements")))
      .selectExpr(transformations.keys.toSeq: _*)

    val namesMap =
      Map("functional_consequence" -> "functionalConsequenceUri")

    val nestedNames = directStringMapping(_, namesMap)

    val reshapedDF =
      ss.createDataFrame(tdf.rdd, H.renameAllCols(tdf.schema, nestedNames compose normAndCCase))

    reshapedDF
  }

  def resolveTargets(df: DataFrame, columnName: String)(
    implicit context: ETLSessionContext): DataFrame = {
    def generateTargetsLUT(df: DataFrame): DataFrame = {
      df.select(
        col("id"),
        mkFlattenArray(
          array(col("id")),
          array(col("proteinAnnotations.id")),
          col("proteinAnnotations.accessions"),
          array(col("approvedSymbol")),
          col("symbolSynonyms")
        ).as("rIds")
      )
        .withColumn("rId", explode(col("rIds")))
        .select("id", "rId")
    }

    logger.info("target resolution evidences and write to out the ones didn't resolve")

    implicit val session = context.sparkSession
    import session.implicits._

    val lut = broadcast(
      Target
        .compute()
        .transform(generateTargetsLUT)
        .orderBy($"rId".asc)
        .repartition($"rId")
    )

    val resolved = df
      .join(lut, col("targetId") === col("rId"), "left_outer")
      .withColumn(columnName, col("id").isNotNull)
      .withColumn("targetId", coalesce(col("id"), col("targetId")))
      .drop("id", "rId")

    resolved
  }

  def resolveDiseases(df: DataFrame, columnName: String)(
    implicit context: ETLSessionContext): DataFrame = {
    logger.info("disease resolution evidences and write to out the ones didn't resolve")

    implicit val session = context.sparkSession
    import session.implicits._

    val lut = broadcast(
      Disease
        .compute()
        .select(col("id").as("dId"))
        .orderBy($"dId".asc)
        .repartition($"dId")
    )

    val resolved = df
      .join(lut, col("diseaseId") === col("dId"), "left_outer")
      .withColumn(columnName, col("dId").isNotNull)
      .drop("dId")

    resolved
  }

  def generateHashes(df: DataFrame, columnName: String)(
    implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    logger.info("validate each evidence generating a hash to check for duplicates")
    val config = context.configuration.evidences

    val commonReqFields = config.uniqueFields.toSet
    val dts = config.dataSources.map { dt =>
      (col("sourceId") === dt.id) -> (commonReqFields ++ dt.uniqueFields.toSet).toList.sorted
        .map(x => col(x).cast(StringType))
    }

    val defaultDts = commonReqFields.toList.sorted.map(x => col(x).cast(StringType))

    val hashes = dts.tail
      .foldLeft(when(dts.head._1, sha1(concat(dts.head._2: _*)))) {
        case op => op._1.when(op._2._1, sha1(concat(op._2._2: _*)))
      }
      .otherwise(sha1(concat(defaultDts: _*)))

    df.withColumn(columnName, hashes)
  }

  def score(df: DataFrame, columnName: String)(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession

    logger.info("validate each evidence generating a hash to check for duplicates")
    val config = context.configuration.evidences

    val dts = config.dataSources.map { dt =>
      (col("sourceId") === dt.id) -> expr(dt.scoreExpr)
    }

    val scores = dts.tail
      .foldLeft(when(dts.head._1, dts.head._2)) {
        case op => op._1.when(op._2._1, op._2._2)
      }
      .otherwise(expr(config.scoreExpr))

    df.withColumn(columnName, scores)
  }

  def markDuplicates(df: DataFrame, hashColumnName: String, columnName: String)(
    implicit context: ETLSessionContext): DataFrame = {
    val idC = col(hashColumnName)
    val w = Window.partitionBy(col("sourceId"), idC).orderBy(idC.asc)

    df.withColumn("_idRank", row_number().over(w))
      .withColumn(columnName, when(col("_idRank") > 1, typedLit(true)).otherwise(false))
      .drop("_idRank")
  }

  def stats(df: DataFrame, sumCols: Seq[(String, Boolean)], uniqueCols: Seq[String])(
    implicit context: ETLSessionContext): DataFrame = {
    import context.sparkSession.implicits._

    val sums = sumCols.map {
      case (n, v) => sum(when(col(n) === v, 1).otherwise(0)).as(s"#$n=$v")
    }

    val uniqs = uniqueCols.map {
      n => countDistinct(col(n)).as(s"#$n")
    }

    val aggs = sums ++ uniqs

    df
      .groupBy($"sourceId")
      .agg(aggs.head, aggs.tail:_*)
  }

  def compute()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession

    val commonSec = context.configuration.common
    val evidencesSec = context.configuration.evidences

    val mappedInputs = Map(
      "evidences" -> H.IOResourceConfig(
        commonSec.inputs.evidence.format,
        commonSec.inputs.evidence.path
      ),
      "rawEvidences" -> H.IOResourceConfig(
        evidencesSec.input.format,
        evidencesSec.input.path
      )
    )
    val dfs = H.readFrom(mappedInputs)

    val rt = "resolvedTarget"
    val rd = "resolvedDisease"
    val md = "markedDuplicate"
    val id = "id"
    val sc = "score"

    val transformedDF = dfs("rawEvidences")
      .transform(reshape)
      .transform(resolveTargets(_, rt))
      .transform(resolveDiseases(_, rd))
      .transform(generateHashes(_, id))
      .transform(score(_, sc))
      .transform(markDuplicates(_, id, md))
      .persist(StorageLevel.DISK_ONLY)

    val okFitler = col(rt) and col(rd) and !col(md)

    Map(
      "evidences/out" -> transformedDF.filter(okFitler).drop(rt, rd, md),
      "evidences/fail" -> transformedDF.filter(not(okFitler)),
//      "evidences/stats" -> transformedDF.filter(not(okFitler))
//        .transform(
//          stats(
//            _,
//            Seq((rt, false), (rd, false), (md, true)),
//            Seq("targetId", "diseaseId")
//          )
//        )
    )
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
