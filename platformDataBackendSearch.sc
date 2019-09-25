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
    "relevance_multiplier")

  implicit class Implicits (val df: DataFrame) {

    def setIdAndSelectFromTargets(evidences: DataFrame): DataFrame = {
      val evsByTarget = evidences.groupBy(col("target_id"))
        .agg(
          count(col("disease_id")).as("num_assocs"),
          first(col("disease_uniq_count")).as("disease_uniq_count"))
        .withColumn("disease_num", col("num_assocs").cast(FloatType) /
          col("disease_uniq_count").cast(FloatType))

      df.withColumn("field_keyword", flatten(array(
        array(col("approved_symbol"),
          col("approved_name"),
          col("hgnc_id"),
          col("id")),
        col("symbol_synonyms"),
        col("name_synonyms"),
        col("uniprot_accessions")
        )))
        .withColumn("field_prefix", flatten(array(
          array(col("approved_symbol"),
            col("approved_name")),
          col("symbol_synonyms"),
          col("name_synonyms")
        )))
        .withColumn("field_ngram", lower(array_join(array_distinct(
          flatten(array(array(col("approved_symbol"), col("approved_name")),
          col("symbol_synonyms"),
          col("name_synonyms"), col("uniprot_function")))), " ")))
        .withColumn("entity", lit("target"))
        .withColumn("category", array(col("biotype")))
        .withColumn("name", col("approved_symbol"))
        .withColumn("description", col("approved_name"))
        .join(evsByTarget, col("id") === col("target_id"), "left_outer")
        .withColumn("relevance_multiplier",
          when(col("disease_num").isNull, lit(0.01))
            .otherwise(log1p(col("disease_num")) + lit(1.0)))
        .selectExpr(searchFields:_*)
    }

    def setIdAndSelectFromDiseases(evidences: DataFrame): DataFrame = {
      val evsByDisease = evidences.groupBy(col("disease_id"))
        .agg(
          count(col("target_id")).as("num_assocs"),
          first(col("target_uniq_count")).as("target_uniq_count"))
        .withColumn("target_num", col("num_assocs").cast(FloatType) /
          col("target_uniq_count").cast(FloatType))

      df.withColumn("field_keyword", flatten(array(array(col("label")), col("efo_synonyms"), array(col("id")))))
        .withColumn("field_prefix", flatten(array(array(col("label")), col("efo_synonyms"))))
        .withColumn("field_ngram", lower(array_join(array_distinct(flatten(array(array(col("label")),
          col("efo_synonyms")))), " ")))
        .withColumn("entity", lit("disease"))
        .withColumn("category", col("therapeutic_labels"))
        .withColumn("name", col("label"))
        .withColumn("description", when(length(col("definition")) === 0,lit(null)).otherwise(col("definition")))
        .join(evsByDisease, col("id") === col("disease_id"), "left_outer")
        .withColumn("relevance_multiplier",
          when(col("target_num").isNull, lit(0.01))
            .otherwise(log1p(col("target_num")) + lit(1.0)))
        .selectExpr(searchFields:_*)
    }

    def setIdAndSelectFromDrugs(evidences: DataFrame): DataFrame = {
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
        .withColumn("field_ngram", lower(array_join(flatten(array_distinct(
          array(
            array(col("pref_name")),
            col("synonyms"),
            col("trade_names"),
            col("descriptions")))),
          " ")))
        // put the drug type in another field
        .withColumn("entity", concat_ws("_", lit("drug"), col("type")))
        .withColumn("category", array(col("type")))
        .withColumn("name", col("pref_name"))
        .withColumn("description", lit(null))
        .withColumn("relevance_multiplier", log1p(col("max_clinical_trial_phase").cast(FloatType) + lit(1.0)))
        .selectExpr(searchFields:_*)
    }

    def aggreateEvidencesByTD: DataFrame = {
      val fevs = df.withColumn("target_id", col("target.id"))
        //.withColumn("disease_id", col("disease.id"))
        .withColumn("score", col("scores.association_score"))
        .where(col("score") > 0.0)

      val totalCounts = fevs.count()

      val pairs = fevs
        .withColumn("disease_id", explode(array_distinct(flatten(col("disease.efo_info.path")))))
        .groupBy(col("target_id"), col("disease_id"))
        .agg(count(col("id")).as("evidence_count"),
          sum(col("score")).as("evidence_score_sum"),
          avg(col("score")).as("evidence_score_avg"))
        .withColumn("evidence_count_total", lit(totalCounts).cast(FloatType))

      val uniqDiseaseCount = pairs.select("disease_id").distinct.count()
      val uniqTargetCount = pairs.select("target_id").distinct.count()

      println(s"uniq diseases: ${uniqDiseaseCount} and uniq targets: ${uniqTargetCount}")

      pairs.withColumn("disease_uniq_count", lit(uniqDiseaseCount))
        .withColumn("target_uniq_count", lit(uniqTargetCount))
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
    .setIdAndSelectFromDrugs(evidences)

//  searchTargets.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/targets/")
//  searchDiseases.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/diseases/")
//  evsAggregatedByTD.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/evidences/")

  val searchObjs = Seq(searchDiseases, searchTargets, searchDrugs)
  val objs = searchObjs.tail.foldLeft(searchObjs.head)((B, left) => B.union(left))

  objs.write.mode(SaveMode.Overwrite).json(outputPathPrefix + "/search/")
}
