import $file.resolvers
import $file.opentargetsFunctions.OpentargetsFunctions._

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
  def apply(synonyms: String, drugs: String, diseases: String, targets: String, output: String) = {
    import SparkSessionWrapper._
    import session.implicits._

    val syns = session.read.parquet(synonyms)
    val dis = session.read.json(diseases)
    val tar = session.read.json(targets)

    val dr = session.read.json(drugs)

    val rawDrugs = dr
      .selectExpr("id as drugId",
        "indications.rows.disease as diseases",
        "flatten(mechanismsOfAction.rows.targets) as targets")
      .withColumn("diseaseId", explode_outer($"diseases"))
      .withColumn("targetId", explode_outer($"targets"))
      .drop("targets", "diseases")
      .withColumn("id", concat_ws("-", $"diseaseId", $"drugId", $"targetId"))

    val leftDrugs = syns
      .filter($"keywordType" === "DS" and $"synonymType" === "CD")
      .selectExpr("keywordId as diseaseId", "synonymId as drugId", "synonymScore as diseaseDrugScore")
    val rightDrugs = syns
      .filter($"keywordType" === "CD" and $"synonymType" === "GP")
      .selectExpr("keywordId as drugId", "synonymId as targetId", "synonymScore as drugTargetScore")

    val jointDrugs = leftDrugs
      .join(rightDrugs, Seq("drugId"))
      .withColumn("score", ($"diseaseDrugScore" + $"drugTargetScore" ) / 2D)
      .withColumn("id", concat_ws("-", $"diseaseId", $"drugId", $"targetId"))
      .join(rawDrugs, Seq("id"), "left_anti")
      .join(tar.selectExpr("id as targetId", "approvedSymbol as symbol"), Seq("targetId"))
      .join(dr.selectExpr("id as drugId", "name as drugName"), Seq("drugId"))
      .join(dis.selectExpr("id as diseaseId", "name as label"), Seq("diseaseId"))
      .drop("id")

    jointDrugs.write.parquet(output + "/retaskFromSynonyms")
  }
}

@main
  def main(synonyms: String, drugs: String, diseases: String, targets: String, output: String): Unit =
    ETL(synonyms, drugs, diseases, targets, output)
