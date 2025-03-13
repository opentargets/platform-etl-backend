package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{flattenCat, nest}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers, Helpers => C}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/** SearchIndex case class to generate search index objects All the search entities require the same
  * structure. Exact matches in the "id", "name", and "keyword" fields are strongly boosted.
  * Otherwise the following fields are used for search with descending score multipliers: "name",
  * "description", "prefixes", "terms5", "terms25", "terms", "ngrams" Score "multiplier" is applied
  * to the non-exact matches.
  * @param id
  *   entity id e.g. target id, disease id, drug id
  * @param name
  *   display name
  * @param description
  *   description of the entry
  * @param entity
  *   entity type e.g. target, disease, drug
  * @param category
  *   category within the entity e.g. therapeutic area, drug type. Used for aggregating the search
  *   results
  * @param keywords
  *   keywords to be used for search i.e. a hit for one of these keywords will return the entry
  * @param prefixes
  * @param ngrams
  * @param terms
  *   top 50 terms
  * @param terms25
  *   Top 25 terms
  * @param terms5
  *   Top 5 terms
  * @param multiplier
  * @param df
  */
case class SearchIndex(
    id: Column,
    name: Column,
    description: Column = lit(null).cast("string"),
    entity: Column,
    category: Column,
    keywords: Column,
    prefixes: Column,
    ngrams: Column,
    terms: Column = lit(Array.empty[String]),
    terms25: Column = lit(Array.empty[String]),
    terms5: Column = lit(Array.empty[String]),
    multiplier: Column = lit(0.01d)
)(implicit val df: DataFrame) {
  val output: DataFrame = df
    .select(
      id as "id",
      name as "name",
      description as "description",
      entity as "entity",
      category as "category",
      keywords as "keywords",
      prefixes as "prefixes",
      ngrams as "ngrams",
      terms as "terms",
      terms25 as "terms25",
      terms5 as "terms5",
      multiplier as "multiplier"
    )
}

object Transformers {
  def processDiseases(efos: DataFrame): DataFrame =
    efos.withColumnRenamed("id", "diseaseId")

  def processTargets(genes: DataFrame): DataFrame =
    genes.withColumnRenamed("id", "targetId")

  /** NOTE finding drugs from associations are computed just using direct assocs otherwise drugs are
    * spread traversing all efo tree. returns Dataframe with ["associationId", "drugIds",
    * "targetId", "diseaseId"]
    */
  def findAssociationsWithDrugs(evidence: DataFrame): DataFrame =
    evidence
      .filter(col("drugId").isNotNull)
      .selectExpr("drugId", "targetId", "diseaseId")
      .withColumn("associationId", concat_ws("-", col("diseaseId"), col("targetId")))
      .groupBy(col("associationId"))
      .agg(
        collect_set(col("drugId")).as("drugIds"),
        first(col("targetId")).as("targetId"),
        first(col("diseaseId")).as("diseaseId")
      )

  // TODO mkarmona finish this
  implicit class Implicits(val df: DataFrame) {
    def setIdAndSelectFromTargets(
        associations: DataFrame,
        associatedDrugs: DataFrame,
        diseases: DataFrame,
        drugs: DataFrame,
        variants: DataFrame
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
      val top25 = 25L // 25
      val top5 = 5L // 5
      val window = Window.partitionBy(col("targetId")).orderBy(col("score").desc)
      val variantWindow = Window
        .partitionBy(col("targetId"))
        .orderBy(col("transcriptScore").asc)

      val assocsWithDrugAndDiseaseLabels = associations
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

      val variantsByTarget = variants
        .withColumn("transcriptConsequences", explode(col("transcriptConsequences")))
        .withColumn(
          "consequenceScore",
          when(col("transcriptConsequences.consequenceScore").isNotNull,
               col("transcriptConsequences.consequenceScore")
          ).otherwise(lit(1))
        )
        .withColumn("targetId", col("transcriptConsequences.targetId"))
        .withColumn("transcriptScore",
                    (col("transcriptConsequences.consequenceScore") + lit(1)) * col(
                      "transcriptConsequences.distanceFromFootprint"
                    )
        )
        .withColumn(
          "variant_labels",
          C.flattenCat(
            "array(variantId)",
            "array(hgvsId)",
            "dbXrefs.id",
            "rsIds"
          )
        )
        .withColumn("variantTargetRank", rank().over(variantWindow))
        .where(col("variantTargetRank") <= top50)
        .groupBy(col("targetId"))
        .agg(
          array_distinct(
            flatten(collect_list(when(col("variantTargetRank") <= top50, col("variant_labels"))))
          )
            .as("variant_labels"),
          array_distinct(
            flatten(collect_list(when(col("variantTargetRank") <= top25, col("variant_labels"))))
          )
            .as("variant_labels_25"),
          array_distinct(
            flatten(collect_list(when(col("variantTargetRank") <= top5, col("variant_labels"))))
          )
            .as("variant_labels_5")
        )
        .persist(StorageLevel.DISK_ONLY)

      val assocsWithLabels = assocsWithDrugAndDiseaseLabels
        .join(variantsByTarget, Seq("targetId"), "left_outer")

      val targetHGNC = df
        .select(
          col("targetId"),
          filter(
            col("dbXRefs"),
            col => col.getField("source") === "HGNC"
          ) as "h"
        )
        .select(col("targetId"), explode_outer(col("h.id")) as "hgncId")
        .withColumn("hgncId", when(col("hgncId").isNotNull, concat(lit("HGNC:"), col("hgncId"))))
        .orderBy("targetId")

      val targets: DataFrame = df
        .join(targetHGNC, Seq("targetId"))
        .join(assocsWithLabels, Seq("targetId"), "left_outer")
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
          "variant_labels",
          when(col("variant_labels").isNull, Array.empty[String])
            .otherwise(col("variant_labels"))
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
          "variant_labels_5",
          when(col("variant_labels_5").isNull, Array.empty[String])
            .otherwise(col("variant_labels_5"))
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
        .withColumn(
          "variant_labels_25",
          when(col("variant_labels_25").isNull, Array.empty[String])
            .otherwise(col("variant_labels_25"))
        )
      SearchIndex(
        id = col("targetId"),
        name = col("approvedSymbol"),
        description = col("approvedName"),
        entity = lit("target"),
        category = array(col("biotype")),
        keywords = C.flattenCat(
          "synonyms.label",
          "proteinIds.id",
          "array(approvedName)",
          "array(approvedSymbol)",
          "array(hgncId)",
          "array(targetId)"
        ),
        prefixes = C.flattenCat(
          "synonyms.label",
          "proteinIds.id",
          "array(approvedName)",
          "array(approvedSymbol)"
        ),
        ngrams = C.flattenCat(
          "proteinIds.id",
          "synonyms.label",
          "array(approvedName)",
          "array(approvedSymbol)"
        ),
        terms = C.flattenCat(
          "disease_labels",
          "drug_labels",
          "variant_labels"
        ),
        terms25 = C.flattenCat(
          "disease_labels_25",
          "drug_labels_25",
          "variant_labels_25"
        ),
        terms5 = C.flattenCat(
          "disease_labels_5",
          "drug_labels_5",
          "variant_labels_5"
        ),
        multiplier =
          when(col("target_relevance").isNotNull, log1p(col("target_relevance")) + lit(1.0d))
            .otherwise(0.01d)
      )(targets).output
    }

    def resolveTALabels(idColumnName: String, taLabelsColumnName: String): DataFrame = {
      val tas = df
        .selectExpr(idColumnName, "therapeuticAreas")
        .withColumn("therapeuticAreaId", explode(col("therapeuticAreas")))
        .orderBy(col("therapeuticAreaId"))
        .join(
          df.selectExpr(s"$idColumnName as therapeuticAreaId", "name as therapeuticAreaLabel"),
          Seq("therapeuticAreaId"),
          "inner"
        )
        .drop("therapeuticAreaId", "therapeuticAreas")
        .groupBy(col(idColumnName))
        .agg(collect_set(col("therapeuticAreaLabel")).as(taLabelsColumnName))
        .persist(StorageLevel.DISK_ONLY)

      df.join(tas, Seq(idColumnName), "left_outer")
    }

    def setIdAndSelectFromDiseases(
        phenotypeNames: DataFrame,
        associations: DataFrame,
        associatedDrugs: DataFrame,
        targets: DataFrame,
        drugs: DataFrame,
        studies: DataFrame
    ): DataFrame = {

      val drugsByDisease = associatedDrugs
        .join(drugs, Seq("drugId"), "inner")
        .groupBy(col("associationId"))
        .agg(array_distinct(flatten(collect_list(col("drug_labels")))).as("drug_labels"))
      // study df exploded by disease id
      val studiesByDisease = studies
        .select(col("studyId"), explode(col("diseaseIds")) as "diseaseId")
        .groupBy(col("diseaseId"))
        .agg(collect_list(col("studyId")).as("studyIds"))

      val top50 = 50L
      val top25 = 25L
      val top5 = 5L
      val window = Window.partitionBy(col("diseaseId")).orderBy(col("score").desc)

      /*
        compute per target all labels from associated diseases and drugs through
        associations
       */
      val assocsWithLabels = associations
        .join(drugsByDisease.drop("diseaseId", "targetId"), Seq("associationId"), "full_outer")
        .withColumn("rank", rank().over(window))
        .where(col("rank") <= top50)
        .join(targets, Seq("targetId"), "inner")
        .groupBy(col("diseaseId"))
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

      val disease = df
        .join(phenotypeNames, Seq("diseaseId"), "left_outer")
        .join(assocsWithLabels, Seq("diseaseId"), "left_outer")
        .join(studiesByDisease, Seq("diseaseId"), "left_outer")
        .withColumn(
          "phenotype_labels",
          when(col("phenotype_labels").isNull, Array.empty[String])
            .otherwise(col("phenotype_labels"))
        )
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
      SearchIndex(
        id = col("diseaseId"),
        name = col("name"),
        description = col("description"),
        entity = lit("disease"),
        category = col("therapeutic_labels"),
        keywords = C.flattenCat(
          "array(name)",
          "array(diseaseId)",
          "synonyms.hasBroadSynonym",
          "synonyms.hasExactSynonym",
          "synonyms.hasNarrowSynonym",
          "synonyms.hasRelatedSynonym"
        ),
        prefixes = C.flattenCat(
          "array(name)",
          "synonyms.hasBroadSynonym",
          "synonyms.hasExactSynonym",
          "synonyms.hasNarrowSynonym",
          "synonyms.hasRelatedSynonym"
        ),
        ngrams = C.flattenCat(
          "array(name)",
          "synonyms.hasBroadSynonym",
          "synonyms.hasExactSynonym",
          "synonyms.hasNarrowSynonym",
          "synonyms.hasRelatedSynonym",
          "phenotype_labels"
        ),
        terms = C.flattenCat(
          "target_labels",
          "drug_labels",
          "studyIds"
        ),
        terms25 = C.flattenCat(
          "target_labels_25",
          "drug_labels_25",
          "studyIds"
        ),
        terms5 = C.flattenCat(
          "target_labels_5",
          "drug_labels_5",
          "studyIds"
        ),
        multiplier =
          when(col("disease_relevance").isNotNull, log1p(col("disease_relevance")) + lit(1.0d))
            .otherwise(0.01d)
      )(disease).output
    }

    def resolveDrugIndications(diseases: DataFrame, outColName: String): DataFrame = {
      // dataframe [indicationId, disease_name]
      val dLabels = diseases
        .selectExpr("diseaseId as indicationId", "disease_name")
        .orderBy(col("indicationId"))

      // dataframe [drugId, indicationId, disease_name]
      val indications = df
        .withColumn("indicationIds", expr("indications.rows.disease"))
        .withColumn("indicationId", explode(col("indicationIds")))
        .selectExpr("drugId", "indicationId")
        .join(dLabels, Seq("indicationId"), "inner")
        .groupBy(col("drugId"))
        .agg(collect_set(col("disease_name")).as(outColName))
        .persist(StorageLevel.DISK_ONLY)
      // df is drug returns dataframe [drugId, indicationId, disease_name ... drug fields]
      df.join(indications, Seq("drugId"), "left_outer")

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
        .agg(
          flatten(collect_list(col("disease_labels"))).as("disease_labels"),
          flatten(collect_list(col("therapeutic_labels"))).as("therapeutic_labels")
        )

      val drugEnrichedWithLabels = tluts
        .join(dluts, Seq("drugId"), "full_outer")
        .orderBy(col("drugId"))

      val drugs = df
        .join(associatedDrugs, col("id") === col("drugId"), "left_outer")
        .withColumn(
          "targetIds",
          when(col("targetIds").isNull, Array.empty[String])
            .otherwise(col("targetIds"))
        )
        .withColumn(
          "diseaseIds",
          when(col("diseaseIds").isNull, Array.empty[String])
            .otherwise(col("diseaseIds"))
        )
        .withColumn("descriptions", expr("rows.mechanismOfAction"))
        .join(broadcast(drugEnrichedWithLabels), Seq("drugId"), "left_outer")
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
      SearchIndex(
        id = col("id"),
        name = col("name"),
        description = col("description"),
        entity = lit("drug"),
        category = array(col("drugType")),
        keywords = C.flattenCat(
          "synonyms",
          "tradeNames",
          "array(name)",
          "array(id)",
          "childChemblIds",
          "crossReferences.PubChem",
          "crossReferences.drugbank",
          "crossReferences.chEBI"
        ),
        prefixes = C.flattenCat(
          "synonyms",
          "tradeNames",
          "array(name)",
          "descriptions"
        ),
        ngrams = C.flattenCat(
          "array(name)",
          "synonyms",
          "tradeNames",
          "descriptions"
        ),
        terms = C.flattenCat(
          "disease_labels",
          "target_labels",
          "indicationLabels",
          "therapeutic_labels"
        ),
        multiplier = when(col("drug_relevance").isNotNull, log1p(col("drug_relevance")) + lit(1.0d))
          .otherwise(0.01d)
      )(drugs).output
    }

    def setIdAndSelectFromVariants(
    ): DataFrame = {
      val variants = df
        .withColumn("locationUnderscore",
                    concat(col("chromosome"), lit("_"), col("position"), lit("_"))
        )
        .withColumn("locationDash", concat(col("chromosome"), lit("-"), col("position"), lit("-")))
        .withColumn("locationColon", concat(col("chromosome"), lit(":"), col("position"), lit(":")))
      SearchIndex(
        id = col("variantId"),
        name = col("variantId"),
        entity = lit("variant"),
        category = array(lit("variant")),
        keywords = C.flattenCat("array(variantId)",
                                "array(hgvsId)",
                                "dbXrefs.id",
                                "rsIds",
                                "array(locationUnderscore)",
                                "array(locationDash)",
                                "array(locationColon)"
        ),
        prefixes = C.flattenCat("array(variantId)",
                                "array(hgvsId)",
                                "dbXrefs.id",
                                "rsIds",
                                "array(locationColon)"
        ),
        ngrams = C.flattenCat("array(variantId)", "dbXrefs.id"),
        multiplier = lit(1.0d)
      )(variants).output
    }

    def setIdAndSelectFromStudies(targets: DataFrame, credibleSets: DataFrame): DataFrame = {
      val studiesWithTargets = df
        .withColumnRenamed("geneId", "targetId")
        .join(targets, Seq("targetId"), "left_outer")

      val window = Window.orderBy(col("credibleSetCount").desc, col("nSamples").desc)
      val studiesWithTargetsAndCredSets = studiesWithTargets
        .join(credibleSets, Seq("studyId"), "left_outer")
        .withColumn("rank", rank().over(window))

      val max = studiesWithTargetsAndCredSets.agg(functions.max("rank")).first().getInt(0)
      val multiplier = expr(s"1 + (($max - rank) / ($max - 1))")

      SearchIndex(
        id = col("studyId"),
        name = col("studyId"),
        entity = lit("study"),
        category = array(lit("study")),
        keywords =
          C.flattenCat("array(studyId)", "array(pubmedId)", "array(publicationFirstAuthor)"),
        prefixes =
          C.flattenCat("array(studyId)", "array(pubmedId)", "array(publicationFirstAuthor)"),
        ngrams = C.flattenCat("array(studyId)"),
        terms5 = C.flattenCat("array(traitFromSource)",
                              "diseaseIds",
                              "array(approvedSymbol)",
                              "array(targetId)"
        ),
        terms25 = C.flattenCat("array(traitFromSource)",
                               "diseaseIds",
                               "array(approvedSymbol)",
                               "array(targetId)"
        ),
        terms = C.flattenCat("array(traitFromSource)",
                             "diseaseIds",
                             "array(approvedSymbol)",
                             "array(targetId)"
        ),
        multiplier = multiplier
      )(studiesWithTargetsAndCredSets).output
    }
  }
}

object Search extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    import Transformers.Implicits

    val searchSec = context.configuration.search
    val mappedInputs = Map(
      "disease" -> searchSec.inputs.diseases,
      "diseasehpo" -> searchSec.inputs.diseaseHpo,
      "hpo" -> searchSec.inputs.hpo,
      "drug" -> searchSec.inputs.drugs.drug,
      "mechanism" -> searchSec.inputs.drugs.mechanismOfAction,
      "indication" -> searchSec.inputs.drugs.indications,
      "evidence" -> searchSec.inputs.evidences,
      "target" -> searchSec.inputs.targets,
      "association" -> searchSec.inputs.associations,
      "variants" -> searchSec.inputs.variants,
      "studies" -> searchSec.inputs.studies,
      "credibleSets" -> searchSec.inputs.credibleSets
    )

    val inputDataFrame = IoHelpers.readFrom(mappedInputs)

    logger.info("process diseases and compute ancestors and descendants and persist")
    val diseases = inputDataFrame("disease").data
      .transform(Transformers.processDiseases)
      .resolveTALabels("diseaseId", "therapeutic_labels")
      .orderBy(col("diseaseId"))
      .persist(StorageLevel.DISK_ONLY)

    logger.info("process hpo and hpo disease and persist")
    val phenotypeNames = inputDataFrame("diseasehpo").data
      .join(inputDataFrame("hpo").data, col("phenotype") === col("id"))
      .select("disease", "phenotype", "name")
      .groupBy("disease")
      .agg(collect_set("name") as "phenotype_labels")
      .withColumnRenamed("disease", "diseaseId")
      .orderBy(col("diseaseId"))
      .persist(StorageLevel.DISK_ONLY)

    logger.info("process targets and persist")
    val targets = inputDataFrame("target").data
      .transform(Transformers.processTargets)
      .orderBy(col("targetId"))
      .persist(StorageLevel.DISK_ONLY)

    logger.info("process drugs and persist")
    val drugs = inputDataFrame("drug").data
      .join(
        inputDataFrame("mechanism").data
          .withColumn("id", explode(col("chemblIds")))
          .transform(
            nest(
              _: DataFrame,
              List("mechanismOfAction", "references", "targetName", "targets"),
              "rows"
            )
          )
          .groupBy("id")
          .agg(
            collect_list("rows") as "rows",
            collect_set("actionType") as "uniqueActionTypes",
            collect_set("targetType") as "uniqueTargetType"
          ),
        Seq("id"),
        "left_outer"
      )
      .join(
        inputDataFrame("indication").data
          .select(
            col("id"),
            col("indications") as "rows",
            col("indicationCount") as "count"
          )
          .transform(nest(_: DataFrame, List("rows", "count"), "indications")),
        Seq("id"),
        "left_outer"
      )
      .withColumnRenamed("id", "drugId")
      .orderBy(col("drugId"))
      .persist(StorageLevel.DISK_ONLY)

    val variants = inputDataFrame("variants").data
      .select("variantId",
              "rsIds",
              "hgvsId",
              "dbXrefs",
              "chromosome",
              "position",
              "transcriptConsequences"
      )

    val studies = inputDataFrame("studies").data
      .select("studyId",
              "traitFromSource",
              "pubmedId",
              "publicationFirstAuthor",
              "diseaseIds",
              "nSamples",
              "geneId"
      )

    // read in the credible sets, extract the studyId field. Then create a column with the count of each studyId
    val credibleSets = inputDataFrame("credibleSets").data
      .select("studyId")
      .groupBy("studyId")
      .agg(count("studyId").cast(DoubleType) as "credibleSetCount")

    val dLUT = diseases
      .withColumn(
        "disease_labels",
        C.flattenCat(
          "array(name)",
          "synonyms.hasBroadSynonym",
          "synonyms.hasExactSynonym",
          "synonyms.hasNarrowSynonym",
          "synonyms.hasRelatedSynonym"
        )
      )
      .selectExpr("diseaseId", "disease_labels", "name as disease_name", "therapeutic_labels")
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
          "rows.mechanismOfAction"
        )
      )
      .select("drugId", "drug_labels")
      .orderBy("drugId")
      .persist(StorageLevel.DISK_ONLY)

    val tLUT = targets
      .withColumn(
        "target_labels",
        C.flattenCat(
          "synonyms.label",
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
    val associationScores = inputDataFrame("association").data
      .withColumn("associationId", concat_ws("-", col("diseaseId"), col("targetId")))
      .withColumnRenamed("overallDatasourceHarmonicScore", "score")
      .select(associationColumns.head, associationColumns.tail: _*)
      .persist(StorageLevel.DISK_ONLY)

    logger.info("find associated drugs using evidence dataset")
    val associationsWithDrugsFromEvidences = inputDataFrame("evidence").data
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
        phenotypeNames,
        associationScores,
        associationsWithDrugsFromEvidencesWithScores,
        tLUT,
        drLUT,
        studies
      )

    logger.info("generate search objects for target entity")
    val searchTargets = targets
      .setIdAndSelectFromTargets(
        associationScores,
        associationsWithDrugsFromEvidencesWithScores,
        dLUT,
        drLUT,
        variants
      )

    logger.info("generate search objects for drug entity")
    val searchDrugs = drugs
      .resolveDrugIndications(
        dLUT,
        "indicationLabels"
      )
      .withColumnRenamed("drugId", "id")
      .setIdAndSelectFromDrugs(associationsWithDrugs, tLUT, dLUT)

    val searchVariants = variants
      .setIdAndSelectFromVariants()
      .repartition(100)

    val searchStudies = studies.setIdAndSelectFromStudies(targets, credibleSets).repartition(100)

    val conf = context.configuration.search
    val outputs = Map(
      "search_diseases" -> IOResource(searchDiseases, conf.outputs.diseases),
      "search_targets" -> IOResource(searchTargets, conf.outputs.targets),
      "search_drugs" -> IOResource(searchDrugs, conf.outputs.drugs),
      "search_variants" -> IOResource(searchVariants, conf.outputs.variants),
      "search_studies" -> IOResource(searchStudies, conf.outputs.studies)
    )

    IoHelpers.writeTo(outputs)
  }
}
