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

}

object ETL extends LazyLogging {

  import ColumnTransformationHelpers._

  object ColumnTransformationHelpers {
    def normalise(c: Column): Column = {
      // https://www.rapidtables.com/math/symbols/greek_alphabet.html
      translate(rtrim(lower(translate(trim(trim(c), "."), "/`''[]{}()- ", "")), "s"),
                "αβγδεζηικλμνξπτυω",
                "abgdezhiklmnxptuo")
    }
  }

  def loadEntities(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val data = sparkSession.read
      .json(uri)
      .withColumn("sentence", explode($"sentences"))
      .drop("sentences")
      .selectExpr("*", "sentence.*")
      .drop("sentence")

    data
  }

  def loadLUTs(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val selectedColumns = Seq(
      $"id".as("keywordId"),
      $"name",
      when($"entity" === "target", lit("GP"))
        .when($"entity" === "disease", lit("DS"))
        .when($"entity" === "drug", lit("CD"))
        .as("type"),
      $"keywords"
    )

    val data = sparkSession.read
      .json(uri)
      .select(selectedColumns: _*)
      .withColumn("keyword", explode($"keywords"))
      .withColumn("labelN", normalise($"keyword"))
      .drop("keywords")
      .orderBy($"type", $"labelN")

    data
  }

  def resolveEntities(entities: DataFrame, luts: DataFrame)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val mergedMatches = entities
      .withColumn("match", explode($"matches"))
      .drop("matches")
      .selectExpr("*", "match.*")
      .drop("match")
      .withColumn("labelN", normalise($"label"))
      .join(luts, Seq("type", "labelN"), "left_outer")
      .withColumn("isMapped", $"keywordId".isNotNull)
      .groupBy($"pmid", $"text")
      .agg(
        first($"organisms").as("organisms"),
        first($"pubDate").as("pubDate"),
        first($"section").as("section"),
        collect_list(
          struct($"endInSentence",
                 $"label",
                 $"sectionEnd",
                 $"sectionStart",
                 $"startInSentence",
                 $"type",
                 $"labelN",
                 $"keywordId",
                 $"isMapped")
        ).as("matches")
      )

    val mergedCooc = entities
      .withColumn("cooc", explode($"co-occurrence"))
      .drop("co-occurrence")
      .selectExpr("*", "cooc.*")
      .drop("cooc")
      .withColumn("label1N", normalise($"label1"))
      .withColumn("label2N", normalise($"label2"))
      .withColumn("type1", substring_index($"type", "-", 1))
      .withColumn("type2", substring_index($"type", "-", -1))
      .drop("type")
      .join(luts, $"type1" === $"type" and $"label1N" === $"labelN", "left_outer")
      .withColumnRenamed("keywordId", "keywordId1")
      .drop("type", "labelN")
      .join(luts, $"type2" === $"type" and $"label2N" === $"labelN", "left_outer")
      .withColumnRenamed("keywordId", "keywordId2")
      .drop("type", "labelN")
      .withColumn("isMapped", $"keywordId1".isNotNull and $"keywordId2".isNotNull)
      .groupBy($"pmid", $"text")
      .agg(
        collect_list(
          struct(
            $"association",
            $"end1",
            $"end2",
            $"evidence_score",
            $"label1",
            $"keywordId1",
            $"label2",
            $"keywordId2",
            $"relation",
            $"start1",
            $"start2",
            concat_ws("-", $"type1", $"type2").as("type"),
            $"type1",
            $"type2",
            $"isMapped"
          )
        ).as("co-occurrence")
      )

    val merged =
      mergedMatches.join(mergedCooc, Seq("pmid", "text"), "left_outer")
      .groupBy($"pmid")
      .agg(
        first($"organisms").as("organisms"),
        first($"pubDate").as("pubDate"),
        collect_list(
          struct(
            $"co-occurrence",
            $"matches",
            $"section",
            $"text"
          )
        ).as("sentences")
      )

    merged
  }

  def apply(entitiesUri: String, lutsUri: String, outputUri: String) = {
    implicit val spark = {
      val ss = SparkSessionWrapper.session
      ss.sparkContext.setLogLevel("WARN")
      ss
    }

    val luts = broadcast(loadLUTs(lutsUri))
    val entities = loadEntities(entitiesUri)
    val resolvedEntities = resolveEntities(entities, luts)

    resolvedEntities.write.json(outputUri)
  }
}

@main
  def main(entitiesUri: String, lutsUri: String, outputUri: String): Unit =
    ETL(entitiesUri, lutsUri, outputUri)
