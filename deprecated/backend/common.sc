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

import scala.collection.mutable.HashMap
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}

import play.api.libs.json.{Json, Reads}

object Helpers extends LazyLogging {
  def mkStringSemantic[T](
      tokens: Seq[T],
      start: String = "",
      sep: String = ", ",
      end: String = "",
      lastSep: String = " and "
  ): Option[String] = {
    val strTokens = tokens.map(_.toString)

    strTokens.size match {
      case 0 => None
      case 1 => Some(strTokens.mkString(start, sep, end))
      case _ =>
        Some(
          (Seq(strTokens.init.mkString(start, sep, "")) :+ lastSep :+ strTokens.last)
            .mkString("", "", end)
        )
    }
  }
}

object ColumnFunctions extends LazyLogging {
  def flattenCat(colNames: String*): Column = {
    val cols = colNames.mkString(",")
    expr(s"""filter(array_distinct(
            | transform(
            |   flatten(
            |     filter(array($cols),
            |       x -> isnotnull(x)
            |     )
            |   ),
            |   s -> replace(trim(s), ',', '')
            | )
            |),
            |t -> isnotnull(t))""".stripMargin)
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

  case class InputInfo(format: String, path: String)
  case class Inputs(
      target: InputInfo,
      disease: InputInfo,
      drug: InputInfo,
      evidence: InputInfo,
      associations: InputInfo,
      ddr: InputInfo,
      reactome: InputInfo,
      eco: InputInfo
  )

  implicit val inputInfoImp = Json.reads[InputInfo]
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

/** EG. val newDF = ss.createDataFrame(df.rdd, renameAllCols(df.schema, renameFcn)) **/
object DataFrameSchemaHelper extends LazyLogging {

  def renameAllCols(schema: StructType, rename: String => String): StructType = {
    def recurRename(schema: StructType): Seq[StructField] = schema.fields.map {
      case StructField(name, dtype: StructType, nullable, meta) =>
        StructField(rename(name), StructType(recurRename(dtype)), nullable, meta)
      case StructField(name, dtype: ArrayType, nullable, meta)
          if dtype.elementType.isInstanceOf[StructType] =>
        StructField(
          rename(name),
          ArrayType(StructType(recurRename(dtype.elementType.asInstanceOf[StructType])), true),
          nullable,
          meta
        )
      case StructField(name, dtype, nullable, meta) =>
        StructField(rename(name), dtype, nullable, meta)
    }
    StructType(recurRename(schema))
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
      inputFileConf: Map[String, Map[String, String]]
  ): Map[String, DataFrame] = {
    logger.info("Load files into Hashmap Dataframe")
    for {
      (stepKey, stepFilename) <- inputFileConf
    } yield (stepKey -> loadFileToDF(stepFilename))
  }

  def loadFileToDF(pathInfo: Map[String, String]): DataFrame = {
    val dataframe = session.read.format(pathInfo("format")).load(pathInfo("path"))
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

  // Replace the spaces from the schema fields with _
  def replaceSpacesSchema(df: DataFrame): DataFrame = {

    //replace all spaces with _
    val renameFcn = (s: String) => s.replaceAll(" ", "_")

    val newDF =
      session.createDataFrame(df.rdd, DataFrameSchemaHelper.renameAllCols(df.schema, renameFcn))

    newDF
  }

}
