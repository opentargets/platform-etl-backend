import $ivy.`ch.qos.logback:logback-classic:1.2.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.2`
import $ivy.`com.typesafe:config:1.4.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.7.3`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import better.files.Dsl._
import better.files._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import play.api.libs.json.{Json, Reads}

object Configuration extends LazyLogging {
  case class DataSource(id: String, weight: Double, dataType: String, propagate: Boolean)
  case class Associations(sparkLogLevel: String, defaultWeight: Double, dataSources: Seq[DataSource])

  implicit val dataSourceImp = Json.reads[DataSource]
  implicit val AssociationImp = Json.reads[Associations]

  def loadObject[T](key: String, config: Config)(implicit tReader: Reads[T]): T = {
    val defaultHarmonicOptions = Json.parse(config.getObject(key)
      .render(ConfigRenderOptions.concise())).as[T]

    defaultHarmonicOptions
  }

  def loadObjectList[T](key: String, config: Config)(implicit tReader: Reads[T]): Seq[T] = {
    val defaultHarmonicDatasourceOptions = config.getObjectList(key).toArray.toSeq.map(el => {
      val co = el.asInstanceOf[ConfigObject]
      Json.parse(co.render(ConfigRenderOptions.concise())).as[T]
    })

    defaultHarmonicDatasourceOptions
  }

  def load: Associations = {
    val config = ConfigFactory.load("opentargets")
    val obj = loadObject[Associations]("ot", config)
    logger.debug(obj.toString)
    obj
  }
}

object Loaders {
  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    val targets = ss.read.json(path)
    targets
  }

  def loadExpressions(path: String)(implicit ss: SparkSession): DataFrame = {
    val expressions = ss.read.json(path)
    expressions
  }

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val diseaseList = ss.read.json(path)

    // generate needed fields as ancestors
    val efos = diseaseList
      .withColumn("disease_id", substring_index(col("code"), "/", -1))
      .withColumn("ancestors", flatten(col("path_codes")))

    // compute descendants
    val descendants = efos
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor", explode(col("ancestors")))
      // all diseases have an ancestor, at least itself
      .groupBy("ancestor")
      .agg(collect_set(col("disease_id")).as("descendants"))
      .withColumnRenamed("ancestor", "disease_id")

    val diseases = efos.join(descendants, Seq("disease_id"))
    diseases
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

object Transformers {
}

@main
def main(expressionFilename: String,
         targetFilename: String,
         diseaseFilename: String,
         evidenceFilename: String,
         outputPathPrefix: String): Unit = {

  // -Dconfig.file=path/to/config-file
  // by default application.conf
  val otc = Configuration.load

  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  // AmmoniteSparkSession.sync()

  import ss.implicits._
  ss.sparkContext.setLogLevel(otc.sparkLogLevel)

  val targets = Loaders.loadTargets(targetFilename)
  val diseases = Loaders.loadDiseases(diseaseFilename)
  val expressions = Loaders.loadExpressions(expressionFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)
}
