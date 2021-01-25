import $file.resolvers

import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
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

  def harmonicFn(c: Column): Column =
    aggregate(
      zip_with(
        sort_array(c, asc = false),
        sequence(lit(1), size(c)),
        (e1, e2)  => e1 / pow(e2, 2D)),
      lit(0D),
      (c1, c2) => c1 + c2
    )
}

object ETL extends LazyLogging {
  def apply(mapped: String, output: String) = {
    import SparkSessionWrapper._
    import session.implicits._

    val data = session.read.json(mapped)
    logger.info("generate parquet co-occurrences")
     data
     .withColumn("sentence", explode($"sentences"))
     .selectExpr("*", "sentence.*").drop("sentence", "sentences", "matches")
     .filter($"co-occurrence".isNotNull)
     .withColumn("cooc", explode($"co-occurrence"))
     .selectExpr("*", "cooc.*").drop("cooc", "co-occurrence")
     .write.parquet(output + "/epmc-cooccurrences")

    logger.info("generate parquet matches")
     data
     .withColumn("sentence", explode($"sentences"))
     .selectExpr("*", "sentence.*").drop("sentence", "sentences", "co-occurrence")
     .filter($"matches".isNotNull).withColumn("match", explode($"matches"))
     .selectExpr("*", "match.*").drop("match", "matches")
     .write.parquet(output + "/epmc-matches")

  }
}

@main
  def main(mapped: String, output: String): Unit =
    ETL(mapped, output)
