import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
// import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:3.0.1`
import $ivy.`org.apache.spark::spark-mllib:3.0.1`
import $ivy.`org.apache.spark::spark-sql:3.0.1`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.9.1`
import $ivy.`com.databricks::spark-xml:0.10.0`

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.databricks.spark.xml._

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

    // case when size(coalesce(transform(filter(PubmedData.ArticleIdList.ArticleId, y -> y._IdType = 'pmc'), x -> x._VALUE),array())) > 0 then element_at(transform(filter(PubmedData.ArticleIdList.ArticleId, y -> y._IdType = 'pmc'), x -> x._VALUE),1) as pmcId
    val columns = List(
      s"MedlineCitation.PMID as ${pmId}",
      s"""
         |case when
         | size(
         |   coalesce(
         |     transform(filter(PubmedData.ArticleIdList.ArticleId, y -> y._IdType = 'pmc'), x -> x._VALUE),
         |     array())
         | ) > 0 then
         | element_at(
         |   transform(filter(PubmedData.ArticleIdList.ArticleId, y -> y._IdType = 'pmc'), x -> x._VALUE),
         |   1
         | )
         |end as ${pmcId}
         |""".stripMargin,
      "MedlineCitation.Article.Abstract.AbstractText",
      "MedlineCitation.Article.ArticleDate",
      "MedlineCitation.Article.ArticleTitle",
      "MedlineCitation.Article.AuthorList",
      "MedlineCitation.Article.Journal",
      "MedlineCitation.Article.Pagination",
      "MedlineCitation.Article.PublicationTypeList",
      "MedlineCitation.DateCompleted",
      "MedlineCitation.DateRevised",
      "MedlineCitation.KeywordList",
      "PubmedData.ArticleIdList",
      "PubmedData.History",
      "PubmedData.ReferenceList"
    )

    // val pubmeds = spark.read.option("rootTag", "PubmedArticleSet").option("rowTag","PubmedArticle").xml("*.xml")
    val data: DataFrame = sparkSession.read
      .option("rootTag", "PubmedArticleSet")
      .option("rowTag", "PubmedArticle")
      .option("excludeAttribute", "true")
      .xml(uri)

    data
      .selectExpr(columns: _*)
      .withColumn("segmentId", getSegmentId(input_file_name()))
  }

  def loadDeletions(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val columns = List(
      "PMID as pmids"
    )

    // val pubmeds = spark.read.option("rootTag", "PubmedArticleSet").option("rowTag","PubmedArticle").xml("*.xml")
    val data: DataFrame = sparkSession.read
      .option("rootTag", "PubmedArticleSet")
      .option("rowTag", "DeleteCitation")
      .option("excludeAttribute", "true")
      .xml(uri)

    data
      .selectExpr(columns: _*)
      .withColumn("segmentId", getSegmentId(input_file_name()))
      .withColumn(pmId, explode($"pmids"))
  }

  def apply(publicationsUri: String, outputUri: String) = {
    implicit val spark = SparkSessionWrapper.session
    spark.sparkContext.setLogLevel("WARN")

    val pmids = loadPublications(publicationsUri)
    val deletions = loadDeletions(publicationsUri)

    // val resolvedEntities = pmids.join(deletions, Seq("pmid"), "left_anti")

    pmids.write.parquet(outputUri + "/pubmed_index")
    deletions.write.parquet(outputUri + "/pubmed_index_deleted")
  }
}

@main
  def main(publicationsUri: String, outputUri: String): Unit =
    ETL(publicationsUri, outputUri)
