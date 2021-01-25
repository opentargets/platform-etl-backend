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
    .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
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

    val evidenceColumns = Seq(
      "pmid",
      "type1",
      "type2",
      "keywordId1",
      "keywordId2",
      "evidence_score"
    )

    val groupedKeys = Seq($"keywordId1", $"keywordId2")

    logger.info("read EPMC co-occurrences dataset, filter only mapped ones and rescale score between 0..1")
    val preA = coos
      .filter($"isMapped" === true and $"type" === "GP-DS")
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .selectExpr(evidenceColumns:_*)

    logger.info("preparing coos A")
    val A = preA
      .transform(makeAssociations(_, groupedKeys))
      .withColumnRenamed("keywordId1", "targetId")
      .withColumnRenamed("keywordId2", "diseaseId")
      .join(dis.selectExpr("diseaseId", "name as label"),
        Seq("diseaseId"))
      .join(tar.selectExpr("targetId", "approvedSymbol as symbol"),
        Seq("targetId"))

    logger.info("generating associations for datasets A (GP - DS)")
    A.write.parquet(output + "/associationsFromCoocsA")
  }
}

@main
  def main(coocs: String, diseases: String, targets: String, output: String): Unit =
    ETL(coocs, diseases, targets, output)
