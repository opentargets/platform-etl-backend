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
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.typesafe.scalalogging.LazyLogging

/**
  * export JAVA_OPTS="-Xms1G -Xmx24G"
  * time amm opentargets-epmc-entity-resolution.sc
  * --entitiesUri ../etl/mkarmona/association/tags_Annot_PMC1240624_PMC1474480_v01.jsonl
  * --lutsUri ../etl/mkarmona/association/search_\\*\\*\/part\\*.json
  * --outputUri mapped_entities/
  */
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

  def makeAssociations(df: DataFrame, groupCols: Seq[Column]): DataFrame = {
    import session.implicits._
    logger.info("compute associations with desc. stats and harmonic (type1 type2 keyword1 keyword2)")
    val assocs = df.groupBy(groupCols:_*)
      .agg(
        first($"label1").as("label1"),
        first($"label2").as("label2"),
        countDistinct($"pmid").as("f"),
        mean($"evidence_score").as("mean"),
        stddev($"evidence_score").as("std"),
        max($"evidence_score").as("max"),
        min($"evidence_score").as("min"),
        expr("approx_percentile(evidence_score, array(0.25, 0.5, 0.75))").as("q"),
        count($"pmid").as("N"),
        collect_list($"evidence_score").as("evidenceScores")
      )
      .withColumn("median", element_at($"q", 2))
      .withColumn("q1", element_at($"q", 1))
      .withColumn("q3", element_at($"q", 3))
      .withColumn("harmonic", harmonicFn($"evidenceScores"))
      .drop("evidenceScores", "q")

    assocs
  }
}

object ETL extends LazyLogging {
  def apply(coocs: String, output: String) = {
    import SparkSessionWrapper._
    import session.implicits._

    val groupedKeys = Seq($"type1", $"type2", $"keywordId1", $"keywordId2")
    val df = session.read.parquet(coocs)
    val assocs = df
      .filter($"isMapped" === true)
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .transform(makeAssociations(_, groupedKeys))

    logger.info("read EPMC co-occurrences dataset, filter only mapped ones and rescale score between 0..1")
    assocs.write.parquet(output + "/associations")

    val groupedKeysWithDate = Seq($"year", $"month", $"type1", $"type2", $"keywordId1", $"keywordId2")
    val assocsPerYear = df
      .withColumn("year", year($"pubDate"))
      .withColumn("month", month($"pubDate"))
      .filter($"isMapped" === true and $"year".isNotNull and $"month".isNotNull)
      .withColumn("evidence_score", array_min(array($"evidence_score" / 10D, lit(1D))))
      .transform(makeAssociations(_, groupedKeysWithDate))

    logger.info("generate assocs but group also per year")
    assocsPerYear.write.parquet(output + "/associationsWithPublicationYearMonth")
  }
}

@main
  def main(coocs: String, output: String): Unit =
    ETL(coocs, output)
