package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{Helpers => C}
import io.opentargets.etl.backend.spark.Helpers._

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
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
    "terms25",
    "terms5",
    "multiplier"
  )

  def processDiseases(efos: DataFrame): DataFrame =
    efos.withColumnRenamed("id", "diseaseId")

  def processTargets(genes: DataFrame): DataFrame =
    genes.withColumnRenamed("id", "targetId")

  def processDrugs(drugs: DataFrame): DataFrame =
    drugs.withColumnRenamed("id", "drugId")

  /** NOTE finding drugs from associations are computed just using direct assocs
    *  otherwise drugs are spread traversing all efo tree.
    *  returns Dataframe with ["association_id", "drug_ids", "target_id", "disease_id"]
    */
  def findAssociationsWithDrugs(evidence: DataFrame): DataFrame = {
    evidence
      .filter(col("drugId").isNotNull)
      .selectExpr(
        "drugId",
        "targetId",
        "diseaseId")
      .withColumn("associationId",
        concat_ws("-", col("diseaseId"), col("targetId")))
      .groupBy(col("associationId"))
      .agg(
        collect_set(col("drugId")).as("drugIds"),
        first(col("targetId")).as("targetId"),
        first(col("diseaseId")).as("diseaseId")
      )
  }

  // TODO mkarmona finish this
  implicit class Implicits(val df: DataFrame) {
    def setIdAndSelectFromTargets(
        associations: DataFrame,
        associatedDrugs: DataFrame,
        diseases: DataFrame,
        drugs: DataFrame
    ): DataFrame = {

      val drugsByTarget = associatedDrugs
        .join(drugs, Seq("drugId"), "inner")
        .groupBy(col("associationId"))
        .agg(array_distinct(flatten(collect_list(col("drug_labels")))).as("drug_labels"))

      /*
        comute per target all labels from associated diseases and drugs through
        associations
       */
      val top50 = 50L
      val top25 = 25L
      val top5 = 5L
      val window = Window.partitionBy(col("targetId")).orderBy(col("score").desc)
      val assocsWithLabels = associations
        .join(drugsByTarget, Seq("associationId"), "left_outer")
        .withColumn("rank", rank().over(window))
        .where(col("rank") <= top50)
        .join(diseases, Seq("diseaseId"), "inner")
        .groupBy(col("targetId"))
        .agg(
          array_distinct(flatten(collect_list(col("disease_labels")))).as("disease_labels"),
          array_distinct(flatten(collect_list(when(col("rank") <= top25, col("disease_labels")))))
            .as("disease_labels_25"),
          array_distinct(flatten(collect_list(when(col("rank") <= top5, col("disease_labels")))))
            .as("disease_labels_5"),
          array_distinct(flatten(collect_list(col("drug_labels")))).as("drug_labels"),
          array_distinct(flatten(collect_list(when(col("rank") <= top25, col("drug_labels")))))
            .as("drug_labels_25"),
          array_distinct(flatten(collect_list(when(col("rank") <= top5, col("drug_labels")))))
            .as("drug_labels_5"),
          mean(col("score")).as("target_relevance")
        )

      df.join(assocsWithLabels, Seq("targetId"), "left_outer")
        .withColumn(
          "disease_labels",
          when(col("disease_labels").isNull, Array.empty[String])
            .otherwise(col("disease_labels"))
        )
        .withColumn(
          "drug_labels",
          when(col("drug_labels").isNull, Array.empty[String])
            .otherwise(col("drug_labels"))
        )
        .withColumn(
          "disease_labels_5",
          when(col("disease_labels_5").isNull, Array.empty[String])
            .otherwise(col("disease_labels_5"))
        )
        .withColumn(
          "drug_labels_5",
          when(col("drug_labels_5").isNull, Array.empty[String])
            .otherwise(col("drug_labels_5"))
        )
        .withColumn(
          "disease_labels_25",
          when(col("disease_labels_25").isNull, Array.empty[String])
            .otherwise(col("disease_labels_25"))
        )
        .withColumn(
          "drug_labels_25",
          when(col("drug_labels_25").isNull, Array.empty[String])
            .otherwise(col("drug_labels_25"))
        )
        .withColumnRenamed("targetId", "id")
        .withColumn(
          "keywords",
          C.flattenCat(
            "symbolSynonyms",
            "nameSynonyms",
            "array(proteinAnnotations.id)",
            "proteinAnnotations.accessions",
            "array(approvedName)",
            "array(approvedSymbol)",
            "array(hgncId)",
            "array(id)"
          )
        )
        .withColumn(
          "prefixes",
          C.flattenCat(
            "nameSynonyms",
            "symbolSynonyms",
            "array(approvedName)",
            "array(approvedSymbol)"
          )
        )
        .withColumn(
          "ngrams",
          C.flattenCat(
            "array(proteinAnnotations.id)",
            "proteinAnnotations.accessions",
            "nameSynonyms",
            "symbolSynonyms",
            "array(approvedName)",
            "array(approvedSymbol)"
          )
        )
        .withColumn(
          "terms",
          C.flattenCat(
            "disease_labels",
            "drug_labels"
          )
        )
        .withColumn(
          "terms25",
          C.flattenCat(
            "disease_labels_25",
            "drug_labels_25"
          )
        )
        .withColumn(
          "terms5",
          C.flattenCat(
            "disease_labels_5",
            "drug_labels_5"
          )
        )
        .withColumn("entity", lit("target"))
        .withColumn("category", array(col("biotype")))
        .withColumn("name", col("approved_symbol"))
        .withColumn("description", col("approved_name"))
        .withColumn(
          "multiplier",
          when(col("target_relevance").isNotNull, log1p(col("target_relevance")) + lit(1.0d))
            .otherwise(0.01d)
        )
        .selectExpr(searchFields: _*)
    }

    def setIdAndSelectFromDiseases(
        associations: DataFrame,
        associatedDrugs: DataFrame,
        targets: DataFrame,
        drugs: DataFrame
    ): DataFrame = {

      val drugsByDisease = associatedDrugs
        .join(drugs, Seq("drug_id"), "inner")
        .groupBy(col("association_id"))
        .agg(array_distinct(flatten(collect_list(col("drug_labels")))).as("drug_labels"))

      val top50 = 50L
      val top25 = 25L
      val top5 = 5L
      val window = Window.partitionBy(col("disease_id")).orderBy(col("score").desc)

      /*
        comute per target all labels from associated diseases and drugs through
        associations
       */
      val assocsWithLabels = associations
        .join(drugsByDisease.drop("disease_id", "target_id"), Seq("association_id"), "full_outer")
        .withColumn("rank", rank().over(window))
        .where(col("rank") <= top50)
        .join(targets, Seq("target_id"), "inner")
        .groupBy(col("disease_id"))
        .agg(
          array_distinct(flatten(collect_list(col("target_labels")))).as("target_labels"),
          array_distinct(flatten(collect_list(col("drug_labels")))).as("drug_labels"),
          array_distinct(flatten(collect_list(when(col("rank") <= top25, col("target_labels")))))
            .as("target_labels_25"),
          array_distinct(flatten(collect_list(when(col("rank") <= top25, col("drug_labels")))))
            .as("drug_labels_25"),
          array_distinct(flatten(collect_list(when(col("rank") <= top5, col("target_labels")))))
            .as("target_labels_5"),
          array_distinct(flatten(collect_list(when(col("rank") <= top5, col("drug_labels")))))
            .as("drug_labels_5"),
          mean(col("score")).as("disease_relevance")
        )

      df.withColumn("phenotype_labels", expr("transform(phenotypes, f -> f.label)"))
        .join(assocsWithLabels, Seq("disease_id"), "left_outer")
        .withColumn(
          "target_labels",
          when(col("target_labels").isNull, Array.empty[String])
            .otherwise(col("target_labels"))
        )
        .withColumn(
          "drug_labels",
          when(col("drug_labels").isNull, Array.empty[String])
            .otherwise(col("drug_labels"))
        )
        .withColumnRenamed("disease_id", "id")
        .withColumn(
          "keywords",
          C.flattenCat(
            "array(label)",
            "array(id)",
            "efo_synonyms"
          )
        )
        .withColumn(
          "prefixes",
          C.flattenCat(
            "array(label)",
            "efo_synonyms"
          )
        )
        .withColumn(
          "ngrams",
          C.flattenCat(
            "array(label)",
            "efo_synonyms",
            "phenotype_labels"
          )
        )
        .withColumn(
          "terms",
          C.flattenCat(
            "target_labels",
            "drug_labels"
          )
        )
        .withColumn(
          "terms25",
          C.flattenCat(
            "target_labels_25",
            "drug_labels_25"
          )
        )
        .withColumn(
          "terms5",
          C.flattenCat(
            "target_labels_5",
            "drug_labels_5"
          )
        )
        .withColumn("entity", lit("disease"))
        .withColumn("category", col("therapeutic_labels"))
        .withColumn("name", col("label"))
        .withColumn(
          "description",
          when(length(col("definition")) === 0, lit(null))
            .otherwise(col("definition"))
        )
        .withColumn(
          "multiplier",
          when(col("disease_relevance").isNotNull, log1p(col("disease_relevance")) + lit(1.0d))
            .otherwise(0.01d)
        )
        .selectExpr(searchFields: _*)
    }

    // uses target_ids, drug_id, target_labels, disease_id, disease_labels
    def setIdAndSelectFromDrugs(
        associatedDrugs: DataFrame,
        targets: DataFrame,
        diseases: DataFrame
    ): DataFrame = {
      val tluts = targets
        .join(
          associatedDrugs.withColumn("targetId", explode(col("targetIds"))),
          Seq("targetId"),
          "inner"
        )
        .groupBy(col("drugId"))
        .agg(flatten(collect_list(col("target_labels"))).as("target_labels"))

      val dluts = diseases
        .join(
          associatedDrugs.withColumn("diseaseId", explode(col("diseaseIds"))),
          Seq("diseaseId"),
          "inner"
        )
        .groupBy(col("drugId"))
        .agg(flatten(collect_list(col("disease_labels"))).as("disease_labels"))

      val drugEnrichedWithLabels = tluts
        .join(dluts, Seq("drugId"), "full_outer")
        .orderBy(col("drugId"))

      val drugs = df
        .join(associatedDrugs, col("id") === col("drugId"), "left_outer")
        .withColumn(
          "target_ids",
          when(col("target_ids").isNull, Array.empty[String])
            .otherwise(col("target_ids"))
        )
        .withColumn(
          "disease_ids",
          when(col("disease_ids").isNull, Array.empty[String])
            .otherwise(col("disease_ids"))
        )
        .withColumn("descriptions", col("mechanisms_of_action.description"))
        .withColumn(
          "keywords",
          C.flattenCat(
            "synonyms",
            "child_chembl_ids",
            "trade_names",
            "array(pref_name)",
            "array(id)"
          )
        )
        .withColumn(
          "prefixes",
          C.flattenCat(
            "synonyms",
            "trade_names",
            "array(pref_name)",
            "descriptions"
          )
        )
        .withColumn(
          "ngrams",
          C.flattenCat(
            "array(pref_name)",
            "synonyms",
            "trade_names",
            "descriptions"
          )
        )
        // put the drug type in another field
        .withColumn("entity", lit("drug"))
        .withColumn("category", array(col("type")))
        .withColumn("name", col("pref_name"))
        .withColumn("description", lit(null))
        .withColumn(
          "multiplier",
          when(col("drug_relevance").isNotNull, log1p(col("drug_relevance")) + lit(1.0d))
            .otherwise(0.01d)
        )
        .join(broadcast(drugEnrichedWithLabels), Seq("drug_id"), "left_outer")
        .withColumn(
          "target_labels",
          when(col("target_labels").isNull, Array.empty[String])
            .otherwise(col("target_labels"))
        )
        .withColumn(
          "disease_labels",
          when(col("disease_labels").isNull, Array.empty[String])
            .otherwise(col("disease_labels"))
        )
        .withColumn("terms25", lit(Array.empty[String]))
        .withColumn("terms5", lit(Array.empty[String]))
        .withColumn(
          "terms",
          C.flattenCat(
            "disease_labels",
            "target_labels",
            "indications.efo_label",
            "indication_therapeutic_areas.therapeutic_label"
          )
        )

      drugs
        .selectExpr(searchFields: _*)
    }
  }
}

object Search extends LazyLogging {
  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import Transformers.Implicits

    val searchSec = context.configuration.search
    val mappedInputs = Map(
      "disease" -> searchSec.inputs.diseases,
      "drug" -> searchSec.inputs.drugs,
      "evidence" -> searchSec.inputs.evidences,
      "target" -> searchSec.inputs.targets,
      "association" -> searchSec.inputs.associations
    )

    val inputDataFrame = C.readFrom(mappedInputs)

    logger.info("process diseases and compute ancestors and descendants and persist")
    val diseases = inputDataFrame("disease")
      .transform(Transformers.processDiseases)
      .orderBy(col("diseaseId"))
      .persist(StorageLevel.DISK_ONLY)

    logger.info("process targets and persist")
    val targets = inputDataFrame("target")
      .transform(Transformers.processTargets)
      .orderBy(col("targetId"))
      .persist(StorageLevel.DISK_ONLY)

    logger.info("process drugs and persist")
    val drugs = inputDataFrame("drug")
      .transform(Transformers.processDrugs)
      .orderBy(col("drugId"))
      .persist(StorageLevel.DISK_ONLY)

    val dLUT = diseases
      .withColumn(
        "disease_labels",
        C.flattenCat(
          "array(name)",
          "synonyms"
        )
      )
      .select("diseaseId", "disease_labels")
      .orderBy("diseaseId")
      .persist(StorageLevel.DISK_ONLY)

    // DataFrame with [drug_id, drug_labels]
    val drLUT: DataFrame = drugs
      .withColumn(
        "drug_labels",
        flattenCat(
          "synonyms",
          "tradeNames",
          "array(name)",
          "mechanismsOfAction.rows.mechanismOfAction"
        )
      )
      .select("drugId", "drug_labels")
      .orderBy("drugId")
      .persist(StorageLevel.DISK_ONLY)

    val tLUT = targets
      .withColumn(
        "target_labels",
        C.flattenCat(
          "symbolSynonyms",
          "nameSynonyms",
          "array(approvedName)",
          "array(approvedSymbol)"
        )
      )
      .select("targetId", "target_labels")
      .orderBy("targetId")
      .persist(StorageLevel.DISK_ONLY)

    val associationColumns = Seq(
      "associationId",
      "targetId",
      "diseaseId",
      "score"
    )

    // TODO check the overall score column name
    logger.info("subselect indirect LLR associations just id and score and persist")
    val associationScores = inputDataFrame("association")
      .withColumn("associationId", concat_ws("-", col("diseaseId"), col("targetId")))
      .withColumnRenamed("overallDatasourceHarmonicScore", "score")
      .select(associationColumns.head, associationColumns.tail: _*)
      .persist(StorageLevel.DISK_ONLY)

    logger.info("find associated drugs using evidence dataset")
    val associationsWithDrugsFromEvidences = inputDataFrame("evidence")
      .transform(Transformers.findAssociationsWithDrugs)

    logger.info("compute total counts for associations and associations with drugs")
    val totalAssociationsWithDrugs = associationsWithDrugsFromEvidences.count()

    val drugColumns = Seq(
      "associationId",
      "drugId",
      "drugIds",
      "targetId",
      "diseaseId",
      "score"
    )
    logger.info("associations with at least 1 drug and the overall score per association")
    val associationsWithDrugsFromEvidencesWithScores = associationsWithDrugsFromEvidences
      .join(associationScores.select("associationId", "score"), Seq("associationId"), "inner")
      .withColumn("drugId", explode(col("drugIds")))
      .select(drugColumns.head, drugColumns.tail: _*)
      .persist(StorageLevel.DISK_ONLY)

    logger.info("collected associations with drugs coming from evidences")
    val associationsWithDrugs = associationsWithDrugsFromEvidencesWithScores
      .groupBy(col("drugId"))
      .agg(
        collect_set(col("targetId")).as("targetIds"),
        collect_set("diseaseId").as("diseaseIds"),
        mean(col("score")).as("meanScore"),
        (count(col("associationId")).cast(DoubleType) / lit(totalAssociationsWithDrugs.toDouble))
          .as("drug_relevance")
      )

    logger.info("generate search objects for disease entity")
    val searchDiseases = diseases
      .setIdAndSelectFromDiseases(
        associationScores,
        associationsWithDrugsFromEvidencesWithScores,
        tLUT,
        drLUT
      )

    logger.info("generate search objects for target entity")
    val searchTargets = targets
      .setIdAndSelectFromTargets(
        associationScores,
        associationsWithDrugsFromEvidencesWithScores,
        dLUT,
        drLUT
      )

    logger.info("generate search objects for drug entity")
    val searchDrugs = drugs
      .withColumnRenamed("drug_id", "id")
      .setIdAndSelectFromDrugs(associationsWithDrugs, tLUT, dLUT)

    val outputs = Seq("search_diseases", "search_targets", "search_drugs")

    val outputConfs = outputs
      .map(
        name =>
          name -> IOResourceConfig(context.configuration.common.outputFormat,
                                   context.configuration.common.output + s"/$name"))
      .toMap

    val outputDFs = (outputs zip Seq(searchDiseases, searchTargets, searchDrugs)).toMap

    C.writeTo(outputConfs, outputDFs)
  }
}
