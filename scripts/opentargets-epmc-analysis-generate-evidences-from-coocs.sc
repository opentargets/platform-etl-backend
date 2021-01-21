import $file.resolvers
import $file.opentargetsFunctions

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
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

import org.graphframes._

object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val session: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate
}

object ETL extends LazyLogging {
  def apply(coocs: String, diseases: String, targets: String, output: String): Unit = {
    import SparkSessionWrapper._
    import session.implicits._

    val coos = session.read.parquet(coocs)
    val dis = broadcast(session.read.json(diseases)
      .withColumnRenamed("id", "diseaseId"))
    val tar = broadcast(session.read.json(targets)
      .withColumnRenamed("id", "targetId"))

    val uniqColumns = Seq(
      "pmid",
      "section",
      "text",
      "start1",
      "end1",
      "start2",
      "end2",
      "targetId",
      "diseaseId"
    )

    val restOfColumns = Seq(
      "resourceScore",
      "score",
      "targetFromSource",
      "diseaseFromSource",
      "literature"
    )

    val evidenceColumns = uniqColumns ++ restOfColumns

    logger.info("read EPMC co-occurrences dataset, filter only unique evidences and map field names")
    val evidences = coos
      .filter($"isMapped" === true and $"type" === "GP-DS")
      .withColumnRenamed("evidence_score", "resourceScore")
      .withColumnRenamed("label1", "targetFromSource")
      .withColumnRenamed("label2", "targetFromDisease")
      .withColumnRenamed("keywordId1", "targetId")
      .withColumnRenamed("keywordId2", "diseaseId")
      .withColumn("score", array_min(array($"resourceScore" / 10D, lit(1D))))
      .withColumn("literature", array($"pmid"))
      .select(evidenceColumns.head, evidenceColumns.tail:_*)
      .dropDuplicates(uniqColumns)

    // TODO JOIN TARGET AND DISEASE

    logger.info("generating evidences for dataset (GP - DS)")
    evidences.write.parquet(output + "/evidencesFromCoocs")
  }
}

@main
  def main(coocs: String, diseases: String, targets: String, output: String): Unit =
    ETL(coocs, diseases, targets, output)
