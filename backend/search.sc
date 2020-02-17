import $file.common
import common._
import common.{ColumnFunctions => C}

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Transformers {
  val searchFields = Seq(
    "id",
    "name",
    "description",
    "entity",
    "category",
    "keywords",
    "prefixes",
    "ngrams",
    "terms",
    "multiplier"
  )

  def processDiseases(efos: DataFrame): DataFrame = {
    val efosDF = efos
      .withColumn("disease_id", substring_index(col("code"), "/", -1))
      .withColumn("ancestors", flatten(col("path_codes")))
      .drop("code", "path_codes")

    // compute descendants
    val descendants = efosDF
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor", explode(col("ancestors")))
      // all diseases have an ancestor, at least itself
      .groupBy("ancestor")
      .agg(collect_set(col("disease_id")).as("descendants"))
      .withColumnRenamed("ancestor", "disease_id")

    val diseases = efosDF.join(descendants, Seq("disease_id"))
    diseases
  }

  def processTargets(genes: DataFrame): DataFrame = {
    val targets = genes
      .withColumnRenamed("id", "target_id")
    targets
  }

  def processDrugs(drugs: DataFrame): DataFrame = {
    val drugDF = drugs
      .withColumnRenamed("id", "drug_id")

    drugDF
  }

  def findAssociationsWithDrugs(evidence: DataFrame, efos: DataFrame): DataFrame = {
    val ancestors = efos.select("disease_id", "ancestors")
    evidence
      .filter(col("drug.id").isNotNull)
      .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
      .selectExpr(
        "drug_id",
        "target.id as target_id",
        "disease.id as disease_id"
      )
      .join(ancestors, Seq("disease_id"), "left_outer")
      .withColumn("ancestor_id", explode(col("ancestors")))
      .withColumn("association_id",
        concat_ws("-", col("target_id"), col("ancestor_id")))
      .groupBy(col("association_id"))
      .agg(collect_set(col("drug_id")).as("drug_ids"),
        first(col("target_id")).as("target_id"),
        first(col("ancestor_id")).as("disease_id"))
  }

  implicit class Implicits(val df: DataFrame) {

    def setIdAndSelectFromTargets(associations: DataFrame,
                                  associatedDrugs: DataFrame,
                                  diseases: DataFrame,
                                  drugs: DataFrame,
                                  associationCounts: Long): DataFrame = {

      val assocsByTarget = associations
        .join(diseases, Seq("disease_id"), "inner")
        .groupBy(col("target_id"))
        .agg(array_distinct(
          flatten(
            collect_list(col("disease_labels")))).as("disease_labels"),
          count(col("id")).as("evs_count"))
        .withColumn("target_relevance", col("evs_count") / lit(associationCounts))

      val drugsByTarget = associatedDrugs
        .join(drugs, Seq("drug_id"), "inner")
        .groupBy(col("target_id"))
        .agg(array_distinct(
          flatten(
            collect_list(col("drug_labels")))).as("drug_labels"))

      /*
        comute per target all labels from associated diseases and drugs through
        associations
       */
      val assocsWithLabels = assocsByTarget
        .join(drugsByTarget, Seq("target_id"), "full_outer")

      df.join(assocsWithLabels, Seq("target_id"), "left_outer")
        .na.fill(0.01D, Seq("target_relevance"))
        .withColumn("disease_labels",
          when(col("disease_labels").isNull, Array.empty[String])
            .otherwise(col("disease_labels")))
        .withColumn("drug_labels",
          when(col("drug_labels").isNull, Array.empty[String])
            .otherwise(col("drug_labels")))
        .withColumnRenamed("target_id", "id")
        .withColumn(
          "keywords",
        C.flattenCat(
          "symbol_synonyms",
          "name_synonyms",
          "uniprot_accessions",
          "array(approved_name)",
          "array(approved_symbol)",
          "array(hgnc_id)",
          "array(id)"
        ))
        .withColumn(
          "prefixes",
          C.flattenCat(
            "name_synonyms",
            "symbol_synonyms",
            "array(approved_name)",
            "array(approved_symbol)"
          ))
        .withColumn(
          "ngrams",
          C.flattenCat(
            "uniprot_function",
            "symbol_synonyms",
            "symbol_synonyms",
            "array(approved_name)",
            "array(approved_symbol)"
          ))
        .withColumn(
          "terms",
          C.flattenCat(
            "disease_labels",
            "drug_labels"
          ))
        .withColumn("entity", lit("target"))
        .withColumn("category", array(col("biotype")))
        .withColumn("name", col("approved_symbol"))
        .withColumn("description", col("approved_name"))
        .withColumn("multiplier", log1p(col("target_relevance")) + lit(1.0D))
        .selectExpr(searchFields: _*)
    }

    def setIdAndSelectFromDiseases(evidences: DataFrame): DataFrame = {
      df.withColumn(
          "keywords",
          array_distinct(flatten(array(array(col("label")), col("efo_synonyms"), array(col("id")))))
        )
        .withColumn(
          "prefixes",
          array_distinct(flatten(array(array(col("label")), col("efo_synonyms"))))
        )
        .withColumn(
          "ngrams",
          array_distinct(flatten(array(array(col("label")), col("efo_synonyms"))))
        )
        .withColumn("entity", lit("disease"))
        .withColumn("category", col("therapeutic_labels"))
        .withColumn("name", col("label"))
        .withColumn(
          "description",
          when(length(col("definition")) === 0, lit(null)).otherwise(col("definition"))
        )
        .join(evidences, col("id") === col("disease_id"), "left_outer")
        .withColumn(
          "multiplier",
          when(col("field_factor").isNull, lit(0.01))
            .otherwise(log1p(col("field_factor")) + lit(1.0))
        )
        .selectExpr(searchFields: _*)
    }

    def setIdAndSelectFromDrugs(associatedDrugs: DataFrame, targets: DataFrame, diseases: DataFrame): DataFrame = {
      val tluts = targets
        .join(associatedDrugs.withColumn("target_id", explode(col("target_ids"))),
          Seq("target_id"), "inner")
        .groupBy(col("drug_id"))
        .agg(flatten(collect_list(col("target_labels"))).as("target_labels"))

      val dluts = diseases
        .join(associatedDrugs.withColumn("disease_id", explode(col("disease_ids"))),
          Seq("disease_id"), "inner")
        .groupBy(col("drug_id"))
        .agg(flatten(collect_list(col("disease_labels"))).as("disease_labels"))

      val drugEnrichedWithLabels = tluts.join(dluts, Seq("drug_id"), "full_outer")
        .orderBy(col("drug_id"))

      val drugs = df
        .join(associatedDrugs, col("id") === col("drug_id"), "left_outer")
        .na.fill(0.01D, Seq("drug_relevance"))
        .withColumn("target_ids",
          when(col("target_ids").isNull, Array.empty[String])
            .otherwise(col("target_ids")))
        .withColumn("disease_ids",
          when(col("disease_ids").isNull, Array.empty[String])
            .otherwise(col("disease_ids")))
        .withColumn("descriptions", col("mechanisms_of_action.description"))
        .withColumn("keywords",
          C.flattenCat(
            "synonyms",
            "child_chembl_ids",
            "trade_names",
            "array(pref_name)",
            "array(id)"))
        .withColumn("prefixes",
          C.flattenCat(
            "synonyms",
            "trade_names",
            "array(pref_name)",
            "descriptions"
          ))
        .withColumn(
          "ngrams",
          C.flattenCat(
            "array(pref_name)",
            "synonyms",
            "trade_names",
            "descriptions"
          ))
        // put the drug type in another field
        .withColumn("entity", lit("drug"))
        .withColumn("category", array(col("type")))
        .withColumn("name", col("pref_name"))
        .withColumn("description", lit(null))
        .withColumn("multiplier", log1p(col("drug_relevance")) + lit(1.0D))

        .join(broadcast(drugEnrichedWithLabels), Seq("drug_id"), "left_outer")
        .withColumn("target_labels",
          when(col("target_labels").isNull, Array.empty[String])
            .otherwise(col("target_labels")))
        .withColumn("disease_labels",
          when(col("disease_labels").isNull, Array.empty[String])
            .otherwise(col("disease_labels")))
        .withColumn(
          "terms",
          C.flattenCat(
            "disease_labels",
            "target_labels",
            "indications.efo_label",
            "indication_therapeutic_areas.therapeutic_label"
          ))

      drugs
        .selectExpr(searchFields: _*)
    }

    def aggreateEvidencesByTD: DataFrame = {
      val fevs = df
        .withColumn("target_id", col("target.id"))
        .withColumn("disease_id", col("disease.id"))
        .withColumn("target_name", col("target.gene_info.name"))
        .withColumn("target_symbol", col("target.gene_info.symbol"))
        .withColumn("disease_name", col("disease.efo_info.label"))
        .withColumn("score", col("scores.association_score"))
        .where((col("score") > 0.0) and (col("sourceID") === "europepmc"))

      val diseaseCounts =
        fevs.groupBy("disease_id").count().withColumnRenamed("count", "disease_count")
      val targetCounts =
        fevs.groupBy("target_id").count().withColumnRenamed("count", "target_count")

      val pairs = fevs
        .groupBy(col("target_id"), col("disease_id"))
        .agg(
          count(col("id")).as("evidence_count"),
          collect_list(col("score")).as("_score_list"),
          first(col("target_symbol")).as("target_symbol"),
          first(col("target_name")).as("target_name"),
          first(col("disease_name")).as("disease_name")
        )
        .withColumn("score_list", slice(sort_array(col("_score_list"), false), 1, 100))
        .withColumn(
          "harmonic",
          expr("""
                 |aggregate(
                 | zip_with(
                 |   score_list,
                 |   sequence(1, size(score_list)),
                 |   (e, i) -> (e / pow(i,2))
                 | ),
                 | 0D,
                 | (a, el) -> a + el
                 |)
                 |""".stripMargin)
        )

      pairs
        .select(
          "target_id",
          "disease_id",
          "target_name",
          "target_symbol",
          "disease_name",
          "harmonic",
          "evidence_count"
        )
        .where("harmonic > 0.0")
        .join(diseaseCounts, Seq("disease_id"), "left_outer")
        .join(targetCounts, Seq("target_id"), "left_outer")
    }

    def aggreateEvidencesByTDDrug: DataFrame = {
      val fevs = df
        .withColumn("target_id", col("target.id"))
        .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
        .withColumn("disease_id", col("disease.id"))
        .withColumn("target_name", col("target.gene_info.name"))
        .withColumn("target_symbol", col("target.gene_info.symbol"))
        .withColumn("disease_name", col("disease.efo_info.label"))
        .withColumn("score", col("scores.association_score"))
        .where((col("score") > 0.0) and (col("sourceID") === "chembl"))

      val tdrdCounts = fevs
        .groupBy("target_id", "drug_id", "disease_id")
        .count()
        .withColumnRenamed("count", "tdrd_count")
      val drugCounts = fevs.groupBy("drug_id").count().withColumnRenamed("count", "drug_count")

      val pairs = fevs
        .groupBy(col("target_id"), col("disease_id"))
        .agg(
          count(col("id")).as("evidence_count"),
          collect_list(col("score")).as("_score_list"),
          collect_set(col("drug_id")).as("drug_ids"),
          first(col("target_name")).as("target_name"),
          first(col("target_symbol")).as("target_symbol"),
          first(col("disease_name")).as("disease_name")
        )
        .withColumn("score_list", slice(sort_array(col("_score_list"), false), 1, 100))
        .withColumn(
          "harmonic",
          expr("""
                 |aggregate(
                 | zip_with(
                 |   score_list,
                 |   sequence(1, size(score_list)),
                 |   (e, i) -> (e / pow(i,2))
                 | ),
                 | 0D,
                 | (a, el) -> a + el
                 |)
                 |""".stripMargin)
        )

      pairs
        .select(
          "target_id",
          "disease_id",
          "target_name",
          "target_symbol",
          "disease_name",
          "harmonic",
          "evidence_count",
          "drug_ids"
        )
        .where("harmonic > 0.0")
        .withColumn("drug_id", explode(col("drug_ids")))
        .join(tdrdCounts, Seq("target_id", "drug_id", "disease_id"), "left_outer")
        .join(drugCounts, Seq("drug_id"), "left_outer")
    }

    def termAndRelevanceFromEvidencePairsByDisease: DataFrame = {
      df.groupBy(col("disease_id"))
        .agg(
          slice(
            sort_array(
              collect_list(
                struct(
                  col("harmonic"),
                  col("target_name"),
                  col("target_symbol"),
                  col("evidence_count").cast(FloatType).as("evidence_count")
                )
              ),
              false
            ),
            1,
            25
          ).as("scored_targets"),
          first(col("disease_count").cast(FloatType)).as("disease_count")
        )
        .withColumn(
          "terms",
          array_distinct(
            array_union(col("scored_targets.target_name"), col("scored_targets.target_symbol"))
          )
        )
        .withColumn(
          "field_factor",
          expr("""
                 |aggregate(scored_targets.evidence_count,
                 | 0D,
                 | (a, x) -> a + x
                 |) / disease_count
                 |""".stripMargin)
        )
        .select("disease_id", "terms", "field_factor")
    }

    def termAndRelevanceFromEvidencePairsByTarget: DataFrame = {
      df.groupBy(col("target_id"))
        .agg(
          slice(
            sort_array(
              collect_list(
                struct(
                  col("harmonic"),
                  col("disease_name"),
                  col("evidence_count").cast(FloatType).as("evidence_count")
                )
              ),
              false
            ),
            1,
            25
          ).as("scored_diseases"),
          first(col("target_count").cast(FloatType)).as("target_count")
        )
        .withColumn("terms", array_distinct(col("scored_diseases.disease_name")))
        .withColumn(
          "field_factor",
          expr("""
                 |aggregate(scored_diseases.evidence_count,
                 | 0D,
                 | (a, x) -> a + x
                 |) / target_count
                 |""".stripMargin)
        )
        .select("target_id", "terms", "field_factor")
    }
  }
}

object Search extends LazyLogging {
  def apply(config: Config)(implicit ss: SparkSession) = {
    import ss.implicits._
    import Transformers.Implicits

    val common = Configuration.loadCommon(config)
    val mappedInputs = Map(
      "disease" -> common.inputs.disease,
      "drug" -> common.inputs.drug,
      "evidence" -> common.inputs.evidence,
      "target" -> common.inputs.target,
      "association" -> common.inputs.association
    )

    val inputDataFrame = SparkSessionWrapper.loader(mappedInputs)

    logger.info("process diseases and compute ancestors and descendants and persist")
    val diseases = Transformers
      .processDiseases(inputDataFrame("disease"))
      .orderBy(col("disease_id"))
      .persist(StorageLevel.DISK_ONLY)

    logger.info("process targets and persist")
    val targets = Transformers
      .processTargets(inputDataFrame("target"))
      .orderBy(col("target_id"))
      .persist(StorageLevel.DISK_ONLY)

    val drugs = Transformers
      .processDrugs(inputDataFrame("drug"))
      .orderBy(col("drug_id"))
      .persist(StorageLevel.DISK_ONLY)

    val dLUT = diseases
      .withColumn("phenotype_labels",
        expr("transform(phenotypes, f -> f.label)"))
      .withColumn("disease_labels",
        C.flattenCat(
          "array(label)",
          "efo_synonyms",
          "phenotype_labels"
        ))
      .select("disease_id", "disease_labels")
      .orderBy("disease_id")
      .persist(StorageLevel.DISK_ONLY)

    val drLUT = drugs
      .withColumn("drug_labels",
        C.flattenCat(
          "symbol_synonyms",
          "name_synonyms",
          "array(approved_name)",
          "array(approved_symbol)"
        ))
      .select("drug_id", "drug_labels")
      .orderBy("drug_id")
      .persist(StorageLevel.DISK_ONLY)

    val tLUT = targets
      .withColumn("labels",
        C.flattenCat(
          "symbol_synonyms",
          "name_synonyms",
          "array(approved_name)",
          "array(approved_symbol)"
        ))
      .select("target_id", "target_labels")
      .orderBy("target_id")
      .persist(StorageLevel.DISK_ONLY)

    val associationColumns = Seq(
      "association_id",
      "target_id",
      "disease_id",
      "score"
    )
    logger.info("subselect associations just id and score and persist")
    val associationScores = inputDataFrame("association")
      .selectExpr(
        "harmonic_sum.overall as score",
        "id as association_id",
        "target.id as target_id",
        "disease.id as disease_id")
//      .orderBy(col("association_id"))
      .select(associationColumns.head, associationColumns.tail:_*)
      .persist(StorageLevel.DISK_ONLY)

    logger.info("find associated drugs using evidence dataset")
    val associationsWithDrugsFromEvidences =
      Transformers
        .findAssociationsWithDrugs(inputDataFrame("evidence"), diseases)

    logger.info("compute total counts for associations and associations with drugs")
    val totalAssociations = associationScores.count()
    val totalAssociationsWithDrugs = associationsWithDrugsFromEvidences.count()

    val drugColumns = Seq(
      "association_id",
      "drug_id",
      "drug_ids",
      "target_id",
      "disease_id",
      "score"
    )
    logger.info("associations with at least 1 drug and the overall score per association")
    val associationsWithDrugsFromEvidencesWithScores = associationsWithDrugsFromEvidences
      .join(associationScores.select("association_id", "score"),
        Seq("association_id"), "inner")
      .withColumn("drug_id", explode(col("drug_ids")))
      .select(drugColumns.head, drugColumns.tail:_*)
      .persist(StorageLevel.DISK_ONLY)

    logger.info("collected associations with drugs coming from evidences")
    val associationsWithDrugs = associationsWithDrugsFromEvidencesWithScores
      .groupBy(col("drug_id"))
      .agg(
        collect_set(col("target_id")).as("target_ids"),
        collect_set("disease_id").as("disease_ids"),
        mean(col("score")).as("mean_score"),
        (count(col("association_id")).cast(DoubleType) / lit(totalAssociationsWithDrugs.toDouble))
          .as("drug_relevance")
      )

    logger.info("generate search objects for drug entity")
    val searchDrugs = drugs
      .withColumnRenamed("drug_id", "id")
      .setIdAndSelectFromDrugs(associationsWithDrugs, tLUT, dLUT)

//    val evsAggregatedByTD = inputDataFrame("evidence").aggreateEvidencesByTD.persist

//    val searchDiseases = diseases
//      .setIdAndSelectFromDiseases(evsAggregatedByTD.termAndRelevanceFromEvidencePairsByDisease)
//
    val searchTargets = targets
      .setIdAndSelectFromTargets(associationScores, associationsWithDrugsFromEvidencesWithScores,
        dLUT, drLUT, totalAssociations)

//    searchDiseases.write.mode(SaveMode.Overwrite).json(common.output + "/search_diseases/")
    logger.info("save search drug entity")
    searchDrugs.write.mode(SaveMode.Overwrite).json(common.output + "/search_drugs/")
    logger.info("save search target entity")
    searchTargets.write.mode(SaveMode.Overwrite).json(common.output + "/search_targets/")
  }
}
