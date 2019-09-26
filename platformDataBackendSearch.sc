import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Loaders {
  /** load a drug datasset from OpenTargets drug index dump */
  def loadDrugs(path: String)(implicit ss: SparkSession): DataFrame = {
    val drugList = ss.read.json(path)
    drugList
  }

  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    val targetList = ss.read.json(path)
    targetList
  }

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val diseaseList = ss.read.json(path)
    diseaseList
      .withColumn("id", substring_index(col("code"), "/", -1))
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

object Transformers {
  val searchFields = Seq("id",
    "name",
    "description",
    "entity",
    "category",
    "field_keyword",
    "field_prefix",
    "field_ngram",
    "field_terms",
    "relevance_multiplier")

  implicit class Implicits (val df: DataFrame) {

    def setIdAndSelectFromTargets(evidences: DataFrame): DataFrame = {
      val evsByTarget = evidences
        .orderBy(col("harmonic").desc)
        .groupBy(col("target_id"))
        .agg(
          collect_list(col("disease_name")).as("diseases"),
          collect_list(col("harmonic")).as("harmonics"),
          sum(col("evidence_count")).as("evidence_sum"),
          first(col("target_count")).as("target_count")
        )
        .withColumn("field_terms", array_distinct(slice(col("diseases"), 1, 25)))
        .withColumn("field_factor", col("evidence_sum").cast(FloatType) / col("target_count").cast(FloatType))

      df.withColumn("field_keyword", array_distinct(flatten(array(
        array(col("approved_symbol"),
          col("approved_name"),
          col("hgnc_id"),
          col("id")),
        col("symbol_synonyms"),
        col("name_synonyms"),
        col("uniprot_accessions")
        ))))
        .withColumn("field_prefix", array_distinct(flatten(array(
          array(col("approved_symbol"),
            col("approved_name")),
          col("symbol_synonyms"),
          col("name_synonyms")
        ))))
        .withColumn("field_ngram", array_distinct(
          flatten(array(array(col("approved_symbol"), col("approved_name")),
          col("symbol_synonyms"),
          col("name_synonyms"), col("uniprot_function")))))
        .withColumn("entity", lit("target"))
        .withColumn("category", array(col("biotype")))
        .withColumn("name", col("approved_symbol"))
        .withColumn("description", col("approved_name"))
        .join(evsByTarget, col("id") === col("target_id"), "left_outer")
        .withColumn("relevance_multiplier",
          when(col("field_factor").isNull, lit(0.01))
            .otherwise(log1p(col("field_factor")) + lit(1.0)))
        .selectExpr(searchFields:_*)
    }

    def setIdAndSelectFromDiseases(evidences: DataFrame): DataFrame = {
      val evsByDisease = evidences
        .orderBy(col("harmonic").desc)
        .groupBy(col("disease_id"))
        .agg(
          collect_list(col("target_name")).as("targets"),
          collect_list(col("target_symbol")).as("targets_symbols"),
          collect_list(col("harmonic")).as("harmonics"),
          sum(col("evidence_count")).as("evidence_sum"),
          first(col("disease_count")).as("disease_count")
        )
        .withColumn("field_terms", array_union(slice(col("targets_symbols"), 1, 25), slice(col("targets"), 1, 25)))
        .withColumn("field_factor", col("evidence_sum").cast(FloatType) / col("disease_count").cast(FloatType))

      df.withColumn("field_keyword", array_distinct(flatten(array(array(col("label")), col("efo_synonyms"), array(col("id"))))))
        .withColumn("field_prefix", array_distinct(flatten(array(array(col("label")), col("efo_synonyms")))))
        .withColumn("field_ngram", array_distinct(flatten(array(array(col("label")),
          col("efo_synonyms")))))
        .withColumn("entity", lit("disease"))
        .withColumn("category", col("therapeutic_labels"))
        .withColumn("name", col("label"))
        .withColumn("description", when(length(col("definition")) === 0,lit(null)).otherwise(col("definition")))
        .join(evsByDisease, col("id") === col("disease_id"), "left_outer")
        .withColumn("relevance_multiplier",
          when(col("field_factor").isNull, lit(0.01))
            .otherwise(log1p(col("field_factor")) + lit(1.0)))
        .selectExpr(searchFields:_*)
    }

    def setIdAndSelectFromDrugs(evidences: DataFrame): DataFrame = {
      val evsByDrug = evidences
        .groupBy(col("drug_id"))
        .agg(
          collect_set(col("target_name")).as("targets"),
          collect_set(col("target_symbol")).as("targets_symbols"),
          collect_set(col("disease_name")).as("diseases"),
          sum(col("drug_evs_ratio")).as("drug_evs_ratio_sum"))
        .withColumn("field_terms", array_union(slice(col("targets_symbols"), 1, 25),
          array_union(slice(col("targets"), 1, 25), slice(col("diseases"), 1, 25))))
        .withColumn("field_factor", col("drug_evs_ratio_sum").cast(FloatType))

      df.withColumn("descriptions", col("mechanisms_of_action.description"))
        .withColumn("field_keyword", flatten(array(col("synonyms"),
        col("child_chembl_ids"),
        col("trade_names"),
        array(col("pref_name"), col("id"))
        )))
        .withColumn("field_prefix", flatten(array(col("synonyms"),
          col("trade_names"),
          array(col("pref_name")),
          col("descriptions"))))
        .withColumn("field_ngram", flatten(array_distinct(
          array(
            array(col("pref_name")),
            col("synonyms"),
            col("trade_names"),
            col("descriptions")))))
        // put the drug type in another field
        .withColumn("entity", lit("drug"))
        .withColumn("category", array(col("type")))
        .withColumn("name", col("pref_name"))
        .withColumn("description", lit(null))
        .join(evsByDrug, col("id") === col("drug_id"), "left_outer")
        .withColumn("relevance_multiplier", when(col("field_factor").isNull, lit(0.01))
          .otherwise(log1p(col("field_factor")) + lit(1.0)))
        .selectExpr(searchFields:_*)
    }

    def aggreateEvidencesByTD: DataFrame = {
      val fevs = df.withColumn("target_id", col("target.id"))
        .withColumn("disease_id", col("disease.id"))
        .withColumn("target_name", col("target.gene_info.name"))
        .withColumn("target_symbol", col("target.gene_info.symbol"))
        .withColumn("disease_name", col("disease.efo_info.label"))
        .withColumn("score", col("scores.association_score"))
        .where((col("score") > 0.0) and (col("sourceID") === "europepmc"))

      val diseaseCounts = fevs.groupBy("disease_id").count().withColumnRenamed("count", "disease_count")
      val targetCounts = fevs.groupBy("target_id").count().withColumnRenamed("count", "target_count")

      val pairs = fevs
//        .withColumn("disease_id", explode(array_distinct(flatten(col("disease.efo_info.path")))))
        .groupBy(col("target_id"), col("disease_id"))
        .agg(count(col("id")).as("evidence_count"),
          collect_list(col("score")).as("_score_list"),
          first(col("target_symbol")).as("target_symbol"),
          first(col("target_name")).as("target_name"),
          first(col("disease_name")).as("disease_name"))
        .withColumn("score_list", slice(sort_array(col("_score_list"), false), 1, 100))
        .withColumn("harmonic",
          expr(
            """
              |aggregate(
              | zip_with(
              |   score_list,
              |   sequence(1, size(score_list)),
              |   (e, i) -> (e / pow(i,2))
              | ),
              | 0D,
              | (a, el) -> a + el
              |)
              |""".stripMargin))

      pairs
        .select("target_id", "disease_id", "target_name", "target_symbol", "disease_name", "harmonic", "evidence_count")
        .where("harmonic > 0.0")
        .join(diseaseCounts, Seq("disease_id"), "left_outer")
        .join(targetCounts, Seq("target_id"), "left_outer")
    }

    def aggreateEvidencesByTDDrug: DataFrame = {
      val fevs = df.withColumn("target_id", col("target.id"))
        .withColumn("drug_id", substring_index(col("drug.id"), "/", -1))
        .withColumn("disease_id", col("disease.id"))
        .withColumn("target_name", col("target.gene_info.name"))
        .withColumn("target_symbol", col("target.gene_info.symbol"))
        .withColumn("disease_name", col("disease.efo_info.label"))
        .withColumn("score", col("scores.association_score"))
        .where((col("score") > 0.0) and (col("sourceID") === "chembl"))

      val tdrdCounts = fevs.groupBy("target_id", "drug_id", "disease_id").count().withColumnRenamed("count", "tdrd_count")
      val drugCounts = fevs.groupBy("drug_id").count().withColumnRenamed("count", "drug_count")

      val pairs = fevs
        //        .withColumn("disease_id", explode(array_distinct(flatten(col("disease.efo_info.path")))))
        .groupBy(col("target_id"), col("disease_id"))
        .agg(count(col("id")).as("evidence_count"),
          collect_list(col("score")).as("_score_list"),
          collect_set(col("drug_id")).as("drug_ids"),
          first(col("target_name")).as("target_name"),
          first(col("target_symbol")).as("target_symbol"),
          first(col("disease_name")).as("disease_name"))
        .withColumn("score_list", slice(sort_array(col("_score_list"), false), 1, 100))
        .withColumn("harmonic",
          expr(
            """
              |aggregate(
              | zip_with(
              |   score_list,
              |   sequence(1, size(score_list)),
              |   (e, i) -> (e / pow(i,2))
              | ),
              | 0D,
              | (a, el) -> a + el
              |)
              |""".stripMargin))

      pairs
        .select("target_id", "disease_id", "target_name", "target_symbol", "disease_name", "harmonic", "evidence_count", "drug_ids")
        .where("harmonic > 0.0")
        .withColumn("drug_id", explode(col("drug_ids")))
        .join(tdrdCounts, Seq("target_id", "drug_id", "disease_id"), "left_outer")
        .join(drugCounts, Seq("drug_id"), "left_outer")
        .withColumn("drug_evs_ratio", col("tdrd_count").cast(FloatType) / col("drug_count").cast(FloatType))
    }
  }
}

@main
def main(drugFilename: String, targetFilename: String, diseaseFilename: String,
         evidenceFilename: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  // AmmoniteSparkSession.sync()

  import ss.implicits._
  import Transformers.Implicits

  val drugs = Loaders.loadDrugs(drugFilename)
  val targets = Loaders.loadTargets(targetFilename)
  val diseases = Loaders.loadDiseases(diseaseFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)

  val evsAggregatedByTD = evidences.aggreateEvidencesByTD.persist

  val searchDiseases = diseases
    .setIdAndSelectFromDiseases(evsAggregatedByTD)

  val searchTargets = targets
    .setIdAndSelectFromTargets(evsAggregatedByTD)

  val searchDrugs = drugs
    .setIdAndSelectFromDrugs(evidences.aggreateEvidencesByTDDrug)

//  searchTargets.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/targets/")
//  searchDiseases.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/diseases/")
//  evsAggregatedByTD.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/evidences/")

  val searchObjs = Seq(searchDiseases, searchTargets, searchDrugs)
  val objs = searchObjs.tail.foldLeft(searchObjs.head)((B, left) => B.union(left))

  objs.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/search/")
}
