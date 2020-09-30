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
    def normalise(c: Column): Column =
      lower(translate(trim(trim(c), "."), "[]{}()'- ", ""))
  }

  def loadEntities(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val data = sparkSession.read
      .json(uri)
      // .withColumn("terms", concat($"GP", $"DS"))
      .withColumn("normalised_gp",
                  transform($"GP",
                            c =>
                              struct(normalise(c).as("term_norm"),
                                     c.as("term_raw"),
                                     lit("target").as("term_type"))))
      .withColumn("normalised_ds",
                  transform($"DS",
                            c =>
                              struct(normalise(c).as("term_norm"),
                                     c.as("term_raw"),
                                     lit("disease").as("term_type"))))
      .withColumn("normalised_terms", concat($"normalised_gp", $"normalised_ds"))
      .withColumn("normalised_term", explode($"normalised_terms"))
      .selectExpr("*", "normalised_term.*")

    data
  }

  def loadLUTs(uri: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val selectedColumns = Seq(
      "id",
      "name",
      "entity",
      "keywords"
    )

    val data = sparkSession.read
      .json(uri)
      .selectExpr(selectedColumns: _*)
      .withColumn("normalised_keywords",
                  transform($"keywords",
                            c => struct(normalise(c).as("keyword_norm"), c.as("keyword_raw"))))
      .withColumn("normalised_keyword", explode(col("normalised_keywords")))
      .withColumnRenamed("entity", "keyword_type")
      .selectExpr("*", "normalised_keyword.*")

    data
  }

  def resolveEntities(entities: DataFrame, luts: DataFrame)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val dict = entities
      .join(luts, $"term_norm" === $"keyword_norm", "left_outer")
      .groupBy($"pmid")
      .agg(
        filter(collect_set(
                 struct($"term_raw",
                        $"term_norm",
                        $"id",
                        $"term_type",
                        $"keyword_raw",
                        $"keyword_type")),
               c => c.getField("id").isNotNull).as("terms_mapped")
      )
      .withColumn("targets_mapped",
                  filter($"terms_mapped",
                         c =>
                           c.getField("term_type") === c.getField("keyword_type") and c.getField(
                             "keyword_type") === "target"))
      .withColumn("diseases_mapped",
                  filter($"terms_mapped",
                         c =>
                           c.getField("term_type") === c.getField("keyword_type") and c.getField(
                             "keyword_type") === "disease"))
      .withColumn(
        "drugs_mapped",
        filter($"terms_mapped",
               c => c.getField("term_type") === "drug" and c.getField("keyword_type") === "drug"))
      .withColumn(
        "cross_mapped",
        filter($"terms_mapped", c => c.getField("term_type") =!= c.getField("keyword_type")))

    dict
  }

  def apply(entitiesUri: String, lutsUri: String, outputUri: String) = {
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
