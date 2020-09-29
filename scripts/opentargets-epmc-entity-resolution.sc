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
  implicit class TransformationHelpers(df: DataFrame)(implicit sparkSession: SparkSession) {
    import sparkSession.implicits._

    def columnNormalisation(toColumnName: String, fromColumnName: String): DataFrame = {
      val cn = col(fromColumnName)
      df.withColumn(toColumnName, array_distinct(transform(cn, c => {
        lower(translate(trim(trim(c), "."), "[]{}()'- ", ""))
      })))
    }
  }

  def loadEntities(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    sparkSession.read.json(uri)
      .withColumn("terms", concat($"GP", $"DS"))
      .columnNormalisation("normalised_terms", "terms")
      .withColumn("normalised_term", explode(col("normalised_terms")))
  }

  def loadLUTs(uri: String)(implicit sparkSession: SparkSession) = {
    val selectedColumns = Seq(
      "id",
      "name",
      "entity",
      "keywords"
    )

    sparkSession.read.json(uri)
      .selectExpr(selectedColumns:_*)
      .columnNormalisation("normalised_keywords", "keywords")
      .withColumn("normalised_keyword", explode(col("normalised_keywords")))
  }

  def resolveEntities(entities: DataFrame, luts: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val dict = entities.join(luts, $"normalised_term" === $"normalised_keyword", "left_outer")
      .groupBy($"pmid")
      .agg(filter(collect_set(struct($"normalised_term", $"id", $"entity")),c => c.getField("id").isNotNull).as("mapped_terms"),
      first($"GP").as("GP"),
      first($"DS").as("DS"))
      .withColumn("targets", filter($"mapped_terms", c => c.getField("entity") === "target"))
      .withColumn("diseases", filter($"mapped_terms", c => c.getField("entity") === "disease"))

    dict
  }

  def apply(entitiesUri: String,
            lutsUri: String,
            outputUri: String) = {
    implicit val spark = SparkSessionWrapper.session

    val entities = loadEntities(entitiesUri)
    val luts = loadLUTs(lutsUri)

    val resolvedEntities = resolveEntities(entities, luts)

    resolvedEntities.write.json(outputUri)
  }
}

@main
def main(entitiesUri: String, lutsUri: String, outputUri: String): Unit =
  ETL(entitiesUri, lutsUri, outputUri)
