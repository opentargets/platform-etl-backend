// how to get the vectors, operate and find synonyms
// val vectors = model.getVectors
//  .filter($"word" isInCollection(Seq(pi3k, atk1, "ENSG00000105221", "ENSG00000140992", "ENSG00000152256")))
//  .agg(Summarizer.sum($"vector").as("v")).select("v").collect.head.getAs[Vector]("v")
// model.findSynonyms(vectors, 10).show()

import $file.resolvers
import $file.opentargetsFunctions
import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
// import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:3.0.1`
import $ivy.`org.apache.spark::spark-mllib:3.0.1`
import $ivy.`org.apache.spark::spark-sql:3.0.1`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.9.1`
import $ivy.`graphframes:graphframes:0.8.1-spark3.0-s_2.12`

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.ml.fpm._
import com.typesafe.scalalogging.LazyLogging

import opentargetsFunctions.OpentargetsFunctions._

object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate
}

object ETL extends LazyLogging {
  val applyModelFn = (model: Broadcast[Word2VecModel], word: String) => {
    try {
      model.value.findSynonymsArray(word, 100).filter(_._2 > 0.1)
    } catch {
      case _ => Array.empty[(String, Double)]
    }
  }

  def normalise(c: Column): Column = {
    // https://www.rapidtables.com/math/symbols/greek_alphabet.html
    sort_array(filter(split(lower(translate(
      rtrim(lower(translate(trim(trim(c), "."), "/`''[]{}()-,", "")), "s"),
      "αβγδεζηικλμνξπτυω",
      "abgdezhiklmnxptuo"
    )), " "), d => length(d) > 2), asc = true)
  }

  private def loadMeddraDf(path: String, columns: Seq[String])(
    implicit ss: SparkSession): DataFrame = {

    val meddraRaw = ss.read.csv(path)
    val meddra = meddraRaw
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$+", ","))
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$$", ""))
      .withColumn("_c0", split(col("_c0"), ","))
      .select(columns.zipWithIndex.map(i => col("_c0").getItem(i._2).as(s"${i._1}")): _*)

    val colsToLower = meddra.columns.filter(_.contains("name"))
    colsToLower.foldLeft(meddra)((df, c) => df.withColumn(c, lower(col(c))))

  }

  def loadMeddraPreferredTerms(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info(s"Loading Meddra preferred terms from $path")
    val cols = Seq("pt_code", "pt_name")
    loadMeddraDf(path + "MedAscii/pt.asc", cols)
  }

  def loadMeddraLowLevelTerms(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading Meddra low level terms from $path")
    val lltCols = Seq("llt_code", "llt_name")
    loadMeddraDf(path + "MedAscii/llt.asc", lltCols)
  }

  def apply(prefix: String, meddra: String, output: String) = {
    import SparkSessionWrapper._
    import spark.implicits._
    implicit val ss: SparkSession = spark

    logger.info("load required datasets from ETL parquet format")
    val diseases = spark.read.parquet(s"${prefix}/diseases")
    val indications = spark.read.parquet(s"${prefix}/drugs/indication")
    val diseasehp = spark.read.parquet(s"${prefix}/disease_hpo")
    val meddraPT = loadMeddraPreferredTerms(meddra)
    val meddraLT = loadMeddraLowLevelTerms(meddra)


    val D = diseases
      .selectExpr("id as efoId", "name as efoName", "synonyms.*")
      .withColumn("efoName", normalise($"efoName"))
      .withColumn("broadSynonyms", concat($"efoName", transform(expr("coalesce(hasBroadSynonym, array())"), c => normalise(c))))
      .withColumn("exactSynonyms", concat($"efoName",transform(expr("coalesce(hasExactSynonym, array())"), c => normalise(c))))
      .withColumn("relatedSynonyms", concat($"efoName", transform(expr("coalesce(hasRelatedSynonym, array())"), c => normalise(c))))
      .withColumn("synonym", explode(array($"broadSynonyms", $"exactSynonyms", $"relatedSynonyms")))
      .filter(length($"synonym") > 3)
      .groupBy($"efoId")
      .agg()

    val MDTerms = ???
    val EDRAGGW2V = new Word2Vec().setWindowSize(10).setNumPartitions(32).setMaxIter(5).setInputCol("v").setOutputCol("predictions")
    val EDRAGGW2VModel = EDRAGGW2V.fit(EDRAGG)
    EDRAGGW2VModel.save(s"${output}/models/EDRAGGW2VModel")

    val EDRAGGW2VModelB = spark.sparkContext.broadcast(EDRAGGW2VModel)
    val EDRAGGFn = applyModelFn(EDRAGGW2VModelB, _)

    EDRAGGW2VModel.getVectors.withColumn("predictions", udf(EDRAGGFn).apply($"word"))
      .withColumn("model", lit("GENETICS"))
      .withColumn("_prediction", explode($"predictions"))
      .withColumn("prediction", $"_prediction".getField("_1"))
      .withColumn("score", $"_prediction".getField("_2"))
      .select(cols.head, cols.tail:_*)
      .write.parquet(s"${output}/models/EDRAGGPredictions")

    // val pr = predictions.groupBy($"word", $"prediction").agg(count($"model").as("n"), mean($"score").as("score"), collect_list($"model").as("m"))
    // val prtops = pr.join(D, !$"prediction".startsWith("CHEMBL") and $"efoId" === $"prediction", "left_outer").join(C, $"prediction".startsWith("CHEMBL") and $"chemblId" === $"prediction", "left_outer").withColumn("name", coalesce($"efoName", $"drugName")).drop("efoId", "chemblId", "efoName", "drugName").orderBy($"word", $"n".desc, $"score".desc)
  }
}

@main
def main(prefix: String, meddra: String, output: String): Unit =
  ETL(prefix, meddra, output)
