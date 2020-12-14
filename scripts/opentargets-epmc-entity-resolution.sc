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
      .withColumn("match", explode($"matches"))
      .drop("matches")
      .selectExpr("*", "match.*")
      .drop("match")
      .withColumn("labelN", normalise($"label"))

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

    val merged = entities
      .join(luts, Seq("type", "labelN"), "left_outer")
      .groupBy($"pmid", $"text")
      .agg(
        first($"organisms").as("organisms"),
        first($"pubDate").as("pubDate"),
        first($"section").as("section"),
        first($"co-occurrence").as("co-occurrence"),
        collect_list(
          struct($"endInSentence",
                 $"label",
                 $"sectionEnd",
                 $"sectionStart",
                 $"startInSentence",
                 $"type",
                 $"labelN",
                 $"keywordId")
        ).as("matches")
      )
      .withColumn(
        "_filteredMatches",
        array_distinct(
          transform(
            filter($"matches", c => c.getField("keywordId").isNotNull),
            a =>
              struct(
                a.getField("label").as("label"),
                a.getField("keywordId").as("keywordId")
            )
          ))
      )
      .withColumn(
        "_matches",
        when(
          $"_filteredMatches".isNotNull and size($"_filteredMatches") > 0,
          map_from_entries($"_filteredMatches")
        )
      )
      .withColumn(
        "coos",
        when(
          $"_matches".isNotNull,
          transform(
            $"co-occurrence",
            f =>
              struct(
                f.getField("association"),
                f.getField("end1"),
                f.getField("end2"),
                f.getField("evidence_score"),
                f.getField("label1"),
                f.getField("label2"),
                f.getField("relation"),
                f.getField("start1"),
                f.getField("start2"),
                f.getField("type"),
                element_at($"_matches", f.getField("label1")).as("keywordId1"),
                element_at($"_matches", f.getField("label2")).as("keywordId2"),
                (element_at($"_matches", f.getField("label1")).isNotNull &&
                  element_at($"_matches", f.getField("label2")).isNotNull).as("mapped")
            )
          )
        )
      )
      .drop("_matches", "_filteredMatches")
      .groupBy($"pmid")
      .agg(
        first($"organisms").as("organisms"),
        first($"pubDate").as("pubDate"),
        collect_list(
          struct(
            $"coos",
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
