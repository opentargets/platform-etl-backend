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

import scala.math.pow

object AssociationHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def computeOntologyExpansion(diseases: DataFrame, datasources: Dataset[DataSource]): DataFrame = {
      val diseaseStruct =
        """
          |named_struct(
          | 'id', disease_id,
          | 'efo_info', named_struct(
          |   'efo_id', code,
          |   'label', label,
          |   'path', path_codes,
          |   'therapeutic_area', named_struct(
          |     'codes', therapeutic_codes,
          |     'labels', therapeutic_labels
          |   )
          | )
          |) as disease
          |""".stripMargin

      // generate needed fields as ancestors
      val lut = diseases.selectExpr(
        "disease_id",
        "descendants",
        diseaseStruct
      )
        .withColumn("descendant", explode(col("descendants")))

      // TODO get a way to avoid propagation
      val dfWithLut = df
        .withColumn("disease_id", expr("disease.id"))
        .drop("disease")
        .join(broadcast(lut), Seq("disease_id"), "inner")

      // we use datasources to exclude some evidences from being propagated
      val ontologyExpanded = dfWithLut
        .join(datasources.select("id", "propagate"),
          col("sourceID") === col("id"), "left_outer")
        .withColumn("descendants",
          when(col("propagate") === false, array(col("disease_id")))
            .otherwise(col("descendants")))
        .drop("propagate")

      ontologyExpanded
    }
    def groupByDataSources(datasources: Dataset[DataSource], otc: AssociationsSection): DataFrame = {
      df
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
          .harmonic("_hs","_v")
          .join(datasources, $"sourceID" === datasources("id"), "left_outer")
          // fill null for weight to default weight in case we have new datasources
          .na.fill(otc.defaultWeight, Seq("weight"))
          .withColumn("_score", $"_hs" * $"weight")
          .withColumn("datasource_score", expr("map(sourceID, _score)"))
    }

    def groupByDataTypes: DataFrame = {
      df.groupBy(
        $"disease_id", $"target_id", $"dataType"
      ).agg(
        first($"target").as("target"),
        first($"disease").as("disease"),
        collect_list($"datasource_count").as("datasource_counts"),
        collect_list($"datasource_score").as("datasource_scores")
      )
        .withColumn("_v", expr("flatten(transform(datasource_scores, x -> map_values(x)))"))
        .harmonic("_hs","_v")
        .withColumn("datatype_count",
          expr("map(dataType, aggregate(flatten(transform(datasource_counts, x -> map_values(x))) ,0D, (a, el) -> a + el))"))
        .withColumn("datatype_score",
          expr("map(dataType, _hs)"))
    }

    def groupByPair: DataFrame = {
      df.groupBy(
        $"disease_id", $"target_id"
      ).agg(
        first($"target").as("target"),
        first($"disease").as("disease"),
        flatten(collect_list($"datasource_counts")).as("datasource_counts"),
        flatten(collect_list($"datasource_scores")).as("datasource_scores"),
        collect_list($"datatype_count").as("datatype_counts"),
        collect_list($"datatype_score").as("datatype_scores")
      ).withColumn("id", concat_ws("-", $"target_id", $"disease_id"))
        .withColumn("_v", expr("flatten(transform(datasource_scores, x -> map_values(x)))"))
        .harmonic("overall","_v")
        .drop("target_id", "disease_id", "_v", "_hs_max")
    }
  }

  implicit class HSHelpers(df: DataFrame) {
    def harmonic(newColName: String, vectorColName: String): DataFrame = {
      val maxVectorElementsDefault: Int = 100
      val pExponentDefault: Int = 2

      def maxHarmonicValue(vSize: Int, pExponent: Int, maxScore: Double): Double =
        (0 until vSize).foldLeft(0D)((acc: Double, n: Int) => acc + (maxScore / pow(1D + n, pExponent)))

      val maxHS = maxHarmonicValue(maxVectorElementsDefault, pExponentDefault, 1.0)
      df.withColumn(newColName,
        expr(
          s"""
            |aggregate(
            | zip_with(
            |   $vectorColName,
            |   sequence(1, size($vectorColName)),
            |   (e, i) -> (e / pow(i,2))
            | ),
            | 0D,
            | (a, el) -> a + el
            |) / $maxHS
            |""".stripMargin))
    }
  }
}

object Configuration extends LazyLogging {
  case class DataSource(id: String, weight: Double, dataType: String, propagate: Boolean)
  case class AssociationsSection(defaultWeight: Double, dataSources: List[DataSource])

  implicit val dataSourceImp = Json.reads[DataSource]
  implicit val AssociationImp = Json.reads[AssociationsSection]

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

  def load: AssociationsSection = {
    logger.info("load configuration from file")

    val config = ConfigFactory.load()
    val obj = loadObject[AssociationsSection]("ot.associations", config)
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
  import AssociationHelpers._

  val datasources = broadcast(otc.dataSources.toDS().orderBy($"id".asc))

//  val targets = Loaders.loadTargets(targetFilename)
//  val diseases = Loaders.loadDiseases(diseaseFilename)
//  val expressions = Loaders.loadExpressions(expressionFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)

  val directPairs = evidences
    .groupByDataSources(datasources, otc)
    .groupByDataTypes
    .groupByPair
    .withColumn("is_direct", lit(true))

  // compute indirect
  val ontology = evidences.computeOntologyExpansion(datasources)
    .withColumn("descendant", explode($"descendants"))
    .drop("descendants")
    .orderBy("descendant")
    .persist

  val indirectPairs = evidences
    .withColumn("did", expr("disease.id"))
    .drop("disease")
    .join(ontology, col("did") === col("descendant"), "inner")
    .drop("descendant", "disease_id")
    .groupByDataSources(datasources, otc)
    .groupByDataTypes
    .groupByPair
    .withColumn("is_direct", lit(false))

  // write to jsonl both direct and indirect
  directPairs.write.json(outputPathPrefix + "/direct/")
  indirectPairs.write.json(outputPathPrefix + "/indirect/")
}
