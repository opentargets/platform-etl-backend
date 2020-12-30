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
  def apply(coocs: String, drugs: String, diseases: String, targets: String, output: String) = {
    import SparkSessionWrapper._
    import session.implicits._

    val coos = session.read.parquet(coocs)
    val dis = session.read.json(diseases).withColumnRenamed("id", "diseaseId")
    val tar = session.read.json(targets).withColumnRenamed("id", "targetId")
    val dr = session.read.json(drugs).withColumnRenamed("id", "drugId")

    logger.info("get all drugs with at least one MOA whether it has indication or not")
    val rawDrugs = dr
      .selectExpr(
        "drugId",
        "indications.rows.disease as diseases",
        "flatten(mechanismsOfAction.rows.targets) as targets"
      )
      .withColumn("indication", explode_outer($"diseases"))
      .withColumn("moa", explode_outer($"targets"))
      .join(dis.selectExpr("diseaseId", "therapeuticAreas"),
        $"indication" === $"diseaseId",
      "left_outer")
      .withColumn("drugTAs", coalesce($"therapeuticAreas", typedLit(Seq.empty[String])))
      .drop("targets", "diseases", "diseaseId", "therapeuticAreas")
      .cache()

    val evidenceColumns = Seq(
      "type1",
      "type2",
      "keywordId1",
      "keywordId2",
      "evidence_score"
    )

    val groupedKeys = Seq($"type1", $"type2", $"keywordId1", $"keywordId2")

    logger.info("read EPMC co-occurrences dataset, filter only mapped ones and rescale score between 0..1")
    val preA = coos
      .filter($"isMapped" === true and $"type" === "GP-DS")
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .selectExpr(evidenceColumns:_*)

    val coosBLeft = coos.filter($"isMapped" === true and $"type" === "GP-CD")
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .selectExpr(
        "type1",
        "type2 as CD",
        "keywordId1",
        "evidence_score as evidence_scoreL"
      )

    val coosBRight = coos.filter($"isMapped" === true and $"type" === "DS-CD")
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .selectExpr(
        "type2 as type1",
        "type1 as CD",
        "keywordId1 as keywordId2",
        "evidence_score as evidence_scoreR"
      )

    val preB = coosBLeft.join(coosBRight, Seq("CD"))
      .withColumn("evidence_score", ($"evidence_scoreL" + $"evidence_scoreR") / 2D)
      .drop("evidence_scoreL", "evidence_scoreR", "CD")
      .selectExpr(evidenceColumns:_*)


    val A = preA
      .transform(makeAssociations(_, groupedKeys))

    val B = preB
      .transform(makeAssociations(_, groupedKeys))

    val AB = preA.unionByName(preB)
      .transform(makeAssociations(_, groupedKeys))

    val assocDisId = "keywordId2"
    val indA = preA
      .transform(makeIndirect(_, assocDisId, dis, "diseaseId"))
      .transform(makeAssociations(_, groupedKeys))

    val indB = preB
      .transform(makeIndirect(_, assocDisId, dis, "diseaseId"))
      .transform(makeAssociations(_, groupedKeys))

    val indAB = preA.unionByName(preB)
      .transform(makeIndirect(_, assocDisId, dis, "diseaseId"))
      .transform(makeAssociations(_, groupedKeys))

    logger.info("generating associations for datasets A (GP - DS")
    A.write.parquet(output + "/associationsFromCoocsA")

    logger.info("generating associations for datasets B (GP - CD - DS)")
    B.write.parquet(output + "/associationsFromCoocsB")

    logger.info("generating associations for datasets A + B")
    AB.write.parquet(output + "/associationsFromCoocsAB")

    logger.info("generating associations for datasets A (GP - DS")
    indA.write.parquet(output + "/associationsFromCoocsIndrectA")

    logger.info("generating associations for datasets B (GP - CD - DS)")
    indB.write.parquet(output + "/associationsFromCoocsIndirectB")

    logger.info("generating associations for datasets A + B")
    indAB.write.parquet(output + "/associationsFromCoocsIndirectAB")

  }
}

@main
  def main(coocs: String, drugs: String, diseases: String, targets: String, output: String): Unit =
    ETL(coocs, drugs, diseases, targets, output)
