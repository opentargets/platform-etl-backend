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
    "entity",
    "field_keyword",
    "field_prefix",
    "field_ngram",
    "relevance_multiplier")

  implicit class Implicits (val df: DataFrame) {
    def setIdAndSelectFromTargets(evidences: DataFrame): DataFrame = {
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
        .withColumn("relevance_multiplier", lit(1.0))
        .selectExpr(searchFields:_*)
    }

    def setIdAndSelectFromDiseases(evidences: DataFrame): DataFrame = {
      df.withColumn("field_keyword", flatten(array(array(col("label")), col("efo_synonyms"), array(col("id")))))
        .withColumn("field_prefix", flatten(array(array(col("label")), col("efo_synonyms"))))
        .withColumn("field_ngram", lower(array_join(array_distinct(flatten(array(array(col("label")),
          col("efo_synonyms")))), " ")))
        .withColumn("entity", lit("disease"))
        .withColumn("relevance_multiplier", lit(1.0))
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
        .withColumn("entity", concat_ws("_", lit("drug") + col("type")))
        .withColumn("relevance_multiplier", col("max_clinical_trial_phase").cast(FloatType))
        .selectExpr(searchFields:_*)
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

  val searchDiseases = diseases
    .setIdAndSelectFromDiseases(evidences)

  val searchTargets = targets
    .setIdAndSelectFromTargets(evidences)

  val searchDrugs = drugs
    .setIdAndSelectFromDrugs(evidences)

  val searchObjs = Seq(searchDiseases, searchTargets, searchDrugs)
  val objs = searchObjs.tail.foldLeft(searchObjs.head)((B, left) => B.union(left))

  objs.write.json(outputPathPrefix + "/search/")
}
