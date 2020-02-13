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
import org.apache.spark.sql._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.HashMap
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}

import play.api.libs.json.{Json, Reads}

object ColumnFunctions extends LazyLogging {
  def flattenCat(colNames: String*): Column = {
    val cols = colNames.mkString(",")
    expr(
      s"""array_distinct(
        |flatten(filter(array($cols), x -> isnotnull(x))
        |))""".stripMargin)
  }
}

object Configuration extends LazyLogging {
  case class DataSource(id: String, weight: Double, dataType: String, propagate: Boolean)
  case class AssociationsSection(
      defaultWeight: Double,
      defaultPropagate: Boolean,
      dataSources: List[DataSource]
  )

  implicit val dataSourceImp = Json.reads[DataSource]
  implicit val AssociationImp = Json.reads[AssociationsSection]

  case class ClinicalTrials(
      studies: String,
      studyReferences: String,
      countries: String,
      sponsors: String,
      interventions: String,
      interventionsOtherNames: String,
      interventionsMesh: String,
      conditions: String,
      conditionsMesh: String
  )
  implicit val clinicalTrialsImp = Json.reads[ClinicalTrials]

  case class Dailymed(rxnormMapping: String, prescriptionData: String)
  implicit val dailymedImp = Json.reads[Dailymed]

  case class EvidenceProteinFix(input: String, output: String)
  implicit val evidenceProteinFixImp = Json.reads[EvidenceProteinFix]

  case class Inputs(
      target: String,
      disease: String,
      drug: String,
      evidence: String,
      association: String,
      ddr: String
  )
  implicit val inputsImp = Json.reads[Inputs]

  case class Common(inputs: Inputs, output: String)
  implicit val commonImp = Json.reads[Common]

  def loadObject[T](key: String, config: Config)(implicit tReader: Reads[T]): T = {
    val defaultHarmonicOptions = Json.parse(
      config
        .getObject(key)
        .render(ConfigRenderOptions.concise())
    )

    logger.debug(s"loaded configuration as json ${Json.asciiStringify(defaultHarmonicOptions)}")
    defaultHarmonicOptions.as[T]
  }

  def loadObjectList[T](key: String, config: Config)(implicit tReader: Reads[T]): Seq[T] = {
    val defaultHarmonicDatasourceOptions = config
      .getObjectList(key)
      .toArray
      .toSeq
      .map(el => {
        val co = el.asInstanceOf[ConfigObject]
        Json.parse(co.render(ConfigRenderOptions.concise())).as[T]
      })

    defaultHarmonicDatasourceOptions
  }

  def load: Config = {
    logger.info("load configuration from file")
    ConfigFactory.load()
  }

  def loadCommon(config: Config): Common = {
    logger.info("load common configuration")
    val obj = loadObject[Common]("ot.common", config)
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }

  def loadClinicalTrials(config: Config): ClinicalTrials = {
    logger.info("load common configuration")
    val obj = loadObject[ClinicalTrials]("ot.clinicalTrials", config)
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }

  def loadDailymed(config: Config): Dailymed = {
    logger.info("load common configuration")
    val obj = loadObject[Dailymed]("ot.dailymed", config)
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }

  def loadAssociationSection(config: Config): AssociationsSection = {
    logger.info("load configuration from file")
    val obj = loadObject[AssociationsSection]("ot.associations", config)
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }

  def loadEvidenceProteinFixSection(config: Config): EvidenceProteinFix = {
    logger.info("load configuration from file")
    val obj = loadObject[EvidenceProteinFix]("ot.evidenceProteinFix", config)
    logger.debug(s"configuration properly case classed ${obj.toString}")

    obj
  }
}

/**
  Spark common functions
  */
object SparkSessionWrapper extends LazyLogging {
  type WriterConfigurator = DataFrameWriter[Row] => DataFrameWriter[Row]

  // Return sensible defaults, possibly modified by configuration if necessary in the future. Eg. parquet
  private def defaultWriterConfigurator(): WriterConfigurator =
    (writer: DataFrameWriter[Row]) => writer.format("json").mode("overwrite")

  logger.info("Spark Session init")
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000")
    .setAppName("etl-generation")
    .setMaster("local[*]")

  lazy val session: SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate

  /** It creates an hashmap of dataframes.
   Es. inputsDataFrame {"disease", Dataframe} , {"target", Dataframe}
   Reading is the first step in the pipeline
    */
  def loader(
      inputFileConf: Map[String, String]
  ): Map[String, DataFrame] = {
    logger.info("Load files into Hashmap Dataframe")
    for {
      (step_key, step_filename) <- inputFileConf
    } yield (step_key -> loadFileToDF(step_filename))
  }

  def loadFileToDF(path: String): DataFrame = {
    val dataframe = session.read.json(path)
    dataframe
  }

  def save(
      df: DataFrame,
      path: String,
      writerConfigurator: Option[WriterConfigurator] = None
  ): DataFrame = {
    val writer =
      writerConfigurator.getOrElse(defaultWriterConfigurator())(df.write)
    writer.save(path.toString)
    logger.info(s"Saved data to '$path'")
    df
  }

}
