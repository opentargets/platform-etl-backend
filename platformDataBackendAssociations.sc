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
  val maxHS = 1.6349839001848923
  case class DataSource(id: String, weight: Double, dataType: String, propagate: Boolean)
  case class Associations(defaultWeight: Double, dataSources: List[DataSource])

  implicit val dataSourceImp = Json.reads[DataSource]
  implicit val AssociationImp = Json.reads[Associations]

  def loadObject[T](key: String, config: Config)(implicit tReader: Reads[T]): T = {
    val defaultHarmonicOptions = Json.parse(config.getObject(key)
      .render(ConfigRenderOptions.concise()))

    logger.debug(s"loaded configuration as json ${Json.asciiStringify(defaultHarmonicOptions)}")
    defaultHarmonicOptions.as[T]
  }

  def loadObjectList[T](key: String, config: Config)(implicit tReader: Reads[T]): Seq[T] = {
    val defaultHarmonicDatasourceOptions = config.getObjectList(key).toArray.toSeq.map(el => {
      val co = el.asInstanceOf[ConfigObject]
      Json.parse(co.render(ConfigRenderOptions.concise())).as[T]
    })

    defaultHarmonicDatasourceOptions
  }

  def load: Associations = {
    logger.info("load configuration from file")

    val config = ConfigFactory.load()
    val obj = loadObject[Associations]("ot.associations", config)
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}

object Loaders extends LazyLogging {
  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info("load targets jsonl")
    val targets = ss.read.json(path)
    targets
  }

  def loadExpressions(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info("load expressions jsonl")
    val expressions = ss.read.json(path)
    expressions
  }

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    logger.info("load diseases jsonl")
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
    logger.info("load evidences jsonl")
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

  val datasources = broadcast(otc.dataSources.toDS().orderBy($"id".asc))

//  val targets = Loaders.loadTargets(targetFilename)
//  val diseases = Loaders.loadDiseases(diseaseFilename)
//  val expressions = Loaders.loadExpressions(expressionFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)

  val pairs = evidences
    .withColumn("disease_id", $"disease.id")
    .withColumn("target_id", $"target.id")
    .withColumn("_score", $"scores.association_score")
    .groupBy($"disease_id", $"target_id", $"sourceID")
    .agg(
      slice(sort_array(collect_list($"_score"), false), 1, 100).as("_v"),
      first($"target").as("target"),
      first($"disease").as("disease"),
      count($"id").as("_count")
    )
    .withColumn("datasource_count", expr("map(sourceID, _count)"))
    .withColumn("_hs_max", lit(Configuration.maxHS))
    .withColumn("_hs",
      expr(
        """
          |aggregate(
          | zip_with(
          |   _v,
          |   sequence(1, size(_v)),
          |   (e, i) -> (e / pow(i,2))
          | ),
          | 0D,
          | (a, el) -> a + el
          |) / _hs_max
          |""".stripMargin))
    .join(datasources, $"sourceID" === datasources("id"), "inner")
    .withColumn("_score", $"_hs" * $"weight")
    .withColumn("datasource_score", expr("map(sourceID, _score)"))

  val assocs = pairs.groupBy(
    $"disease_id", $"target_id", $"dataType"
  ).agg(
    first($"target").as("target"),
    first($"disease").as("disease"),
    collect_list($"datasource_count").as("datasource_counts"),
    collect_list($"datasource_score").as("datasource_scores")
  )
    .withColumn("_v", expr("flatten(transform(datasource_scores, x -> map_values(x)))"))
    .withColumn("_hs_max", lit(Configuration.maxHS))
    .withColumn("_hs",
      expr(
        """
          |aggregate(
          | zip_with(
          |   slice(sort_array(_v, false), 1, 100),
          |   sequence(1, size(_v)),
          |   (e, i) -> (e / pow(i,2))
          | ),
          | 0D,
          | (a, el) -> a + el
          |) / _hs_max
          |""".stripMargin))
    .withColumn("datatype_count",
      expr("map(dataType, aggregate(flatten(transform(datasource_counts, x -> map_values(x))) ,0D, (a, el) -> a + el))"))
    .withColumn("datatype_score",
      expr("map(dataType, _hs)"))

  val assocsPrima = assocs.groupBy(
    $"disease_id", $"target_id"
  ).agg(
    first($"target").as("target"),
    first($"disease").as("disease"),
    first($"datasource_counts").as("datasource_counts"),
    first($"datasource_scores").as("datasource_scores"),
    collect_list($"datatype_count").as("datatype_counts"),
    collect_list($"datatype_score").as("datatype_scores")
  )

  assocsPrima.write.json(outputPathPrefix)
}
