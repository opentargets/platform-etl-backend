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
  def apply(synonyms: String, drugs: String, diseases: String, targets: String, output: String) = {
    import SparkSessionWrapper._
    import session.implicits._

    val syns = session.read.parquet(synonyms)
    val dis = session.read.json(diseases)
//    val tar = session.read.json(targets)
    val dr = session.read.json(drugs)

    logger.info("get all drugs with at least one MOA whether it has indication or not")
    val rawDrugs = dr
      .selectExpr("id as dId",
        "indications.rows.disease as diseases",
        "flatten(mechanismsOfAction.rows.targets) as targets")
      .withColumn("indication", explode_outer($"diseases"))
      .withColumn("moa", explode_outer($"targets"))
      .join(dis.selectExpr("id as diseaseId", "therapeuticAreas"),
        $"indication" === $"diseaseId",
      "left_outer")
      .withColumn("drugTAs", coalesce($"therapeuticAreas", typedLit(Seq.empty[String])))
      .drop("targets", "diseases", "diseaseId", "therapeuticAreas")
      .cache()

    logger.info("get all suggested genes per disease matched")
    val indicationsFromDS = syns
      .filter($"keywordType" === "DS" and $"synonymType" === "GP")
      .selectExpr("keywordId as diseaseId", "synonymId as targetId", "synonymScore as score")
      .join(rawDrugs.filter($"moa".isNotNull),
        $"targetId" === $"moa" and $"diseaseId" =!= $"indication")
      .withColumn("type", lit("DS->GP<-Drug"))
      .select("diseaseId", "dId", "score", "type", "drugTAs")

    logger.info("get all suggested diseases per drug matched")
    val diseasesFromCD = syns
      .filter($"keywordType" === "CD" and $"synonymType" === "DS")
      .selectExpr("keywordId as drugId", "synonymId as diseaseId", "synonymScore as score")
      .join(rawDrugs, $"drugId" === $"dId" and $"diseaseId" =!= $"indication")
      .withColumn("type", lit("Drug->CD->DS"))
      .select("diseaseId", "dId", "score", "type", "drugTAs")

    logger.info("get all suggested drugs per disease matched")
    val drugsFromDS = syns
      .filter($"keywordType" === "DS" and $"synonymType" === "CD")
      .selectExpr("keywordId as diseaseId", "synonymId as drugId", "synonymScore as score")
      .join(rawDrugs, $"drugId" === $"dId" and $"diseaseId" =!= $"indication")
      .withColumn("type", lit("DS->CD<-Drug"))
      .select("diseaseId", "dId", "score", "type", "drugTAs")

    val mergedDF = Seq(diseasesFromCD, drugsFromDS)
      .foldLeft(indicationsFromDS){
        (B, df) => B.unionByName(df)
      }
      .groupBy($"diseaseId", $"dId")
      .agg(
        count($"type").as("counts"),
        collect_set($"type").as("types"),
        collect_list($"score").as("scores"),
        array_distinct(flatten(collect_list($"drugTAs"))).as("drugTAs")
      )
      .withColumn("harmonic", harmonicFn($"scores"))
      .drop("scores")
      .join(dis.selectExpr("id as diseaseId", "name as label", "coalesce(therapeuticAreas, array()) as diseaseTAs"),
        Seq("diseaseId"))
      .join(dr.selectExpr("id as dId", "name as name"), Seq("dId"))
      .withColumn("newTAs", array_except( $"diseaseTAs", $"drugTAs"))
      .withColumn("hasNewTA", size($"newTAs") > 0)
      .drop("id", "diseaseTAs", "drugTAs")
      .orderBy($"dId".asc, $"harmonic".desc)

    mergedDF.write.json(output + "/retaskFromSynonyms")
  }
}

@main
  def main(synonyms: String, drugs: String, diseases: String, targets: String, output: String): Unit =
    ETL(synonyms, drugs, diseases, targets, output)
