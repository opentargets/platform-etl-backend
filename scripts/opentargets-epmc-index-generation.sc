import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
// import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:3.0.1`
import $ivy.`org.apache.spark::spark-mllib:3.0.1`
import $ivy.`org.apache.spark::spark-sql:3.0.1`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.9.1`

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.typesafe.scalalogging.LazyLogging

/**
  * export JAVA_OPTS="-Xms1G -Xmx24G"
  */
object SparkSessionWrapper extends LazyLogging {
  logger.info("Spark Session init")
  lazy val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .setAppName("epmc-index-generation")
    .setMaster("local[*]")

  lazy val session: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate

}

object ETL extends LazyLogging {
  val pmId = "pmId"
  val pmcId = "pmcId"

  val getSegmentId: Column => Column = c => substring_index(substring_index(c, "/", -1), ".", 1)

  def loadPublications(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val columns = List(
      s"PMID.`$$` as ${pmId}",
      s"element_at(transform(filter(ids, i -> i.IdType = 'pmc'),j -> j.`$$`), 1) as ${pmcId}",
      "title",
      "abstract",
      "authors",
      "dataCompleted as dateCompleted",
      "date",
      "dateHistory",
      "journal",
      "pagination",
      "keywords",
      "publicationTypes"
    )

    // val pubmeds = spark.read.option("rootTag", "PubmedArticleSet").option("rowTag","PubmedArticle").xml("*.xml")
    val data: DataFrame = sparkSession.read
      .json(uri)

    data
      .selectExpr(columns: _*)
      .withColumn(pmcId, ltrim(col(pmcId), "PMC").cast(LongType))
      .withColumn("segmentId", getSegmentId(input_file_name()))
  }

  def loadDeletions(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    // val pubmeds = spark.read.option("rootTag", "PubmedArticleSet").option("rowTag","PubmedArticle").xml("*.xml")
    val data: DataFrame = sparkSession.read
      .option("header", "false")
      .csv(uri)
      .toDF(pmId)
      .withColumn(s"$pmId", col(pmId).cast(LongType))

    data.orderBy(col(pmId)).repartition(col(pmId))
  }

  def apply(publicationsUri: String, deletionsUri: String, outputUri: String) = {
    implicit val spark = SparkSessionWrapper.session
    spark.sparkContext.setLogLevel("WARN")

    val pmids = loadPublications(publicationsUri)
    val deletions = broadcast(loadDeletions(deletionsUri))

    val resolvedEntities = pmids.join(deletions, Seq(pmId), "left_anti")

    resolvedEntities.write.parquet(outputUri + "/pubmed_index")
  }
}

@main
  def main(publicationsUri: String, deletionsUri: String, outputUri: String): Unit =
    ETL(publicationsUri, deletionsUri, outputUri)
