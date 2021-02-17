package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.OTConfig
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

import scala.util.Random

/** Options to be used by Spark to configure dataframe loading. */
case class IOResourceConfigOption(k: String, v: String)

/** Combines data and metadata regarding storage/retrieval */
case class IOResource(data: DataFrame, configuration: IOResourceConfig)

/**
  * Specifies resource to be used in the ETL.
  *
  * @param format      used to help Spark know the format of the incoming file
  * @param path        to resource
  * @param options     configuration options
  * @param partitionBy partition results by
  */
case class IOResourceConfig(
    format: String,
    path: String,
    options: Option[Seq[IOResourceConfigOption]] = None,
    partitionBy: Option[Seq[String]] = None
)

object CsvHelpers {
  val tsvWithHeader: Option[Seq[IOResourceConfigOption]] = Some(
    Seq(IOResourceConfigOption("header", "true"), IOResourceConfigOption("sep", "\t")))
}

object IoHelpers extends LazyLogging {
  type WriterConfigurator = DataFrameWriter[Row] => DataFrameWriter[Row]
  type IOResourceConfigurations = Map[String, IOResourceConfig]
  type IOResources = Map[String, IOResource]

  /** Create an IOResourceConf Map for each of the given files, where the file is a key and the value is the output
    * configuration
    * @param files will be the names out the output files
    * @param configuration to provide access to the program's configuration
    * @return a map of file -> IOResourceConfig
    */
  def generateDefaultIoOutputConfiguration(
      files: String*
  )(configuration: OTConfig): IOResourceConfigurations = {
    files.map { n =>
      n -> IOResourceConfig(configuration.common.outputFormat, configuration.common.output + s"/$n")
    } toMap
  }

  /** It creates an hashmap of dataframes.
    *   Es. inputsDataFrame {"disease", Dataframe} , {"target", Dataframe}
    *   Reading is the first step in the pipeline
    */
  def readFrom(
      inputFileConf: IOResourceConfigurations
  )(implicit session: SparkSession): IOResources = {
    logger.info("Load files into a Map of names and IOResource")
    for {
      (key, formatAndPath) <- inputFileConf
    } yield key -> IOResource(loadFileToDF(formatAndPath), formatAndPath)
  }

  def loadFileToDF(pathInfo: IOResourceConfig)(implicit session: SparkSession): DataFrame = {
    logger.info(s"load dataset ${pathInfo.path} with ${pathInfo.toString}")

    pathInfo.options
      .foldLeft(session.read.format(pathInfo.format)) {
        case ops =>
          val options = ops._2.map(c => c.k -> c.v).toMap
          ops._1.options(options)
      }
      .load(pathInfo.path)
  }

  /**
    * Helper function to prepare multiple files of the same category to be read by `readFrom`
    * @param resourceConfigs collection of IOResourceConfig of unknown composition
    * @return Map with random keys to input resource.
    */
  def seqToIOResourceConfigMap(resourceConfigs: Seq[IOResourceConfig]): IOResourceConfigurations = {
    (for (rc <- resourceConfigs) yield Random.alphanumeric.take(6).toString -> rc).toMap
  }

  def writeTo(outputs: IOResources)(implicit session: SparkSession): IOResources = {
    val datasetNamesStr = outputs.keys.mkString("(", ", ", ")")
    logger.info(s"write datasets $datasetNamesStr")
    outputs foreach { out =>
      logger.info(s"save dataset ${out._1} with ${out._2.toString}")

      val data = out._2.data
      val conf = out._2.configuration

      val pb = conf.partitionBy.foldLeft(data.write) {
        case (df, ops) =>
          logger.debug(s"enabled partition by ${ops.toString}")
          df.partitionBy(ops: _*)
      }

      conf.options
        .foldLeft(pb) {
          case (df, ops) =>
            logger.debug(s"write to ${conf.path} with options ${ops.toString}")
            val options = ops.map(c => c.k -> c.v).toMap
            df.options(options)

        }
        .format(conf.format)
        .save(conf.path)

    }

    outputs
  }
}
