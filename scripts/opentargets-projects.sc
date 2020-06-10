import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.5`
import $ivy.`org.apache.spark::spark-mllib:2.4.5`
import $ivy.`org.apache.spark::spark-sql:2.4.5`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import $ivy.`com.typesafe.play::play-json:2.8.1`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.typesafe.scalalogging.LazyLogging

/**
  Spark common functions
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
  def getProjects(uri: String)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val columns = Map(
      "`OTAR code`" -> "otar_code",
      "`Project name`" -> "project_name",
      "status" -> "status",
      "`EFO Disease ID`" -> "efo_code",
      "concat('http://home.opentargets.org/', `OTAR code`)" -> "reference"
    )

    val expressions = columns.map(p => p._1 + " as " + p._2).toList
    val selectedColumns = columns.filterNot(p => p._2 == "efo_code").map(p => col(p._2)).toList

    val projects = ss.read
      .option("sep", "\t")
      .option("mode", "DROPMALFORMED")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(uri)

    projects
      .where($"EFO Disease ID".isNotNull)
      .selectExpr(expressions:_*)
      .withColumn("project", struct(selectedColumns:_*))
      .select("efo_code", "project")
  }

  def getDiseases(uri: String)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val diseases = ss.read.json(uri)
      .withColumn("id", substring_index(col("code"), "/", -1))
      .withColumn(
        "ancestors",
        array_except(
          array_distinct(flatten(col("path_codes"))),
          array(col("id"))
        )
      )

    val descendants = diseases
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor",
        explode(concat(array(col("id")), col("ancestors"))))
      .groupBy("ancestor")
      .agg(collect_set(col("id")).as("descendants"))
      .withColumnRenamed("ancestor", "id")
      .withColumn("descendants",
        array_except(
          col("descendants"),
          array(col("id"))
        ))

    val efos = diseases
      .join(descendants, Seq("id"), "left")

    efos.select("id", "ancestors", "descendants")
  }

  def apply(diseaseUri: String, projectsUri: String, outputUri: String) = {
    implicit val spark = SparkSessionWrapper.session

    val diseases = getDiseases(diseaseUri)
    val projects = getProjects(projectsUri)

    val aggProjects = projects.join(diseases, col("efo_code") === col("id"), "inner")
      .withColumn("ancestor",
        explode(concat(array(col("id")), col("ancestors"))))
      .groupBy(col("ancestor").as("efo_id"))
      .agg(collect_set(col("project")).as("projects"))

    aggProjects
      .coalesce(1)
      .write.json(outputUri)
  }
}

/* It needs the tsv project from the google speadsheet
  https://docs.google.com/spreadsheets/d/1CV_shXJy1ACM09HZBB_-3Nl6l_dfkrA26elMAF0ttHs/edit#gid=1496263064
  also the disease json dump from (keep up to date with the latest version)
  gsutil -m cp gs://ot-snapshots/jsonl/20.04/20.04_efo-data.json .
 */
@main
def main(diseaseUri: String,
         projectsUri: String,
         outputUri: String): Unit =
  ETL(diseaseUri, projectsUri, outputUri)
