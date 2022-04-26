package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.OTConfig
import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

import scala.util.Random

/** Options to be used by Spark to configure dataframe loading. */
case class IOResourceConfigOption(k: String, v: String)

/** Combines data and metadata regarding storage/retrieval */
case class IOResource(data: DataFrame, configuration: IOResourceConfig)

/** Specifies resource to be used in the ETL.
  *
  * @param format           used to help Spark know the format of the incoming file
  * @param path             to resource
  * @param options          configuration options
  * @param partitionBy      partition results by
  * @param generateMetadata whether the resource needs associated metadata.
  */
case class IOResourceConfig(
    format: String,
    path: String,
    options: Option[Seq[IOResourceConfigOption]] = None,
    partitionBy: Option[Seq[String]] = None,
    generateMetadata: Boolean = true
)

case class Metadata(
    id: String,
    resource: IOResourceConfig,
    serialisedSchema: String,
    columns: List[String]
)

object CsvHelpers {
  val tsvWithHeader: Option[Seq[IOResourceConfigOption]] = Some(
    Seq(IOResourceConfigOption("header", "true"), IOResourceConfigOption("sep", "\t"))
  )
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
      .foldLeft(session.read.format(pathInfo.format)) { case ops =>
        val options = ops._2.map(c => c.k -> c.v).toMap
        ops._1.options(options)
      }
      .load(pathInfo.path)
  }

  /** Helper function to prepare multiple files of the same category to be read by `readFrom`
    *
    * @param resourceConfigs collection of IOResourceConfig of unknown composition
    * @return Map with random keys to input resource.
    */
  def seqToIOResourceConfigMap(resourceConfigs: Seq[IOResourceConfig]): IOResourceConfigurations = {
    (for (rc <- resourceConfigs) yield Random.alphanumeric.take(6).toString -> rc).toMap
  }

  private def writeTo(output: IOResource)(implicit context: ETLSessionContext): IOResource = {
    implicit val spark: SparkSession = context.sparkSession

    val writeMode = context.configuration.sparkSettings.writeMode
    logger.debug(s"Write mode set to $writeMode")

    logger.info(s"save IOResource ${output.toString}")
    val data = output.data
    val conf = output.configuration

    val pb = conf.partitionBy.foldLeft(data.write) { case (df, ops) =>
      logger.debug(s"enabled partition by ${ops.toString}")
      df.partitionBy(ops: _*)
    }

    conf.options
      .foldLeft(pb) { case (df, ops) =>
        logger.debug(s"write to ${conf.path} with options ${ops.toString}")
        val options = ops.map(c => c.k -> c.v).toMap
        df.options(options)

      }
      .format(conf.format)
      .mode(writeMode)
      .save(conf.path)

    output
  }

  /** writeTo save all datasets in the Map outputs. It does write per IOResource
    * its companion metadata dataset
    *
    * @param outputs the Map with all IOResource
    * @param context the context to have the configuration and the spark session
    * @return the same outputs as a continuator
    */
  def writeTo(outputs: IOResources)(implicit context: ETLSessionContext): IOResources = {
    implicit val spark: SparkSession = context.sparkSession

    val datasetNamesStr = outputs.keys.mkString("(", ", ", ")")
    logger.info(s"write datasets $datasetNamesStr")

    outputs foreach { out =>
      logger.info(s"save dataset ${out._1}")
      writeTo(out._2)

      if (out._2.configuration.generateMetadata) {
        logger.info(s"save metadata for dataset ${out._1}")
        val md = generateMetadata(out._2, context.configuration.common.metadata)
        writeTo(md)
      }
    }

    outputs
  }

  /** Given an IOResource ior and the metadata config section it generates a one-line DF that
    * will be saved coalesced to 1 into a folder inside the metadata output folder. This will
    * make easier to collect the matadata of the created resources
    *
    * @param ior        the IOResource from which generates the metadata
    * @param withConfig the metadata Config section
    * @param context    ETL context object
    * @return a new IOResource with all needed information and data ready to be saved
    */
  private def generateMetadata(ior: IOResource, withConfig: IOResourceConfig)(implicit
      context: ETLSessionContext
  ): IOResource = {
    require(withConfig.path.nonEmpty, "metadata resource path cannot be empty")
    implicit val session: SparkSession = context.sparkSession
    import session.implicits._

    val serialisedSchema = ior.data.schema.json
    val iores = ior.configuration.copy(
      path = ior.configuration.path
        .replace(context.configuration.common.output, "")
        .split("/")
        .filter(_.nonEmpty)
        .mkString("/", "/", "")
    )

    val cols = ior.data.columns.toList
    val id = ior.configuration.path.split("/").filter(_.nonEmpty).last
    val newPath = withConfig.path + s"/$id"
    val metadataConfig = withConfig.copy(path = newPath)

    val metadata =
      List(Metadata(id, iores, serialisedSchema, cols)).toDF
        .withColumn("timeStamp", current_timestamp())
        .coalesce(numPartitions = 1)

    val metadataIOResource = IOResource(metadata, metadataConfig)

    logger.info(s"generate metadata info for $id in path $newPath")

    metadataIOResource
  }

}
