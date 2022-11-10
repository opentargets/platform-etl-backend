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
  * @param format
  *   used to help Spark know the format of the incoming file
  * @param path
  *   to resource
  * @param options
  *   configuration options
  * @param partitionBy
  *   partition results by
  * @param generateMetadata
  *   whether the resource needs associated metadata.
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

  lazy val metadataHelper: ETLSessionContext => MetadataWriter = new MetadataHelper(_)

  /** It creates an hashmap of dataframes. Es. inputsDataFrame {"disease", Dataframe} , {"target",
    * Dataframe} Reading is the first step in the pipeline
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
    * @param resourceConfigs
    *   collection of IOResourceConfig of unknown composition
    * @return
    *   Map with random keys to input resource.
    */
  def seqToIOResourceConfigMap(resourceConfigs: Seq[IOResourceConfig]): IOResourceConfigurations =
    (for (rc <- resourceConfigs) yield Random.alphanumeric.take(6).toString -> rc).toMap
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

  /** Add additional output formats to prepared IOResources. Each dataframe will be cached to
    * prevent recalculation on a subsequent call to write.
    *
    * Additional formats are set in the configuration under `common.additional-outputs`. When there
    * are entries here, each output is given an additional configuration to facilitate writing in
    * multiple output formats (eg, json and parquet).
    * @param resources
    *   standard collection of resources to save
    * @param additionalFormats
    *   additional output configurations
    * @param defaultFormat
    *   default format for writing outputs
    * @return
    *   IOResources of outputs to save. This includes all the entries in `resources` and an
    *   additional entry for each item in `config`.
    */
  private def addAdditionalOutputFormats(
      resources: IOResources,
      additionalFormats: List[String],
      defaultFormat: String
  ): IOResources = {

    val cachedResources: IOResources =
      resources.mapValues(r => IOResource(r.data.cache(), r.configuration))

    cachedResources ++ cachedResources.flatMap { kv =>
      val (name, resource) = kv
      additionalFormats.map { additionalFormat =>
        val resourceName = resource.configuration.path.replace(defaultFormat, additionalFormat)
        val key = s"${name}_$additionalFormat"
        val value = IOResource(
          resource.data,
          resource.configuration
            .copy(
              format = additionalFormat,
              path = resourceName
            )
        )
        key -> value
      }
    }
  }

  /** writeTo save all datasets in the Map outputs. It does write per IOResource its companion
    * metadata dataset
    *
    * @param outputs
    *   the Map with all IOResource
    * @param context
    *   the context to have the configuration and the spark session
    * @return
    *   the same outputs as a continuator
    */
  def writeTo(outputs: IOResources)(implicit context: ETLSessionContext): IOResources = {
    implicit val spark: SparkSession = context.sparkSession

    // add in additional output types
    val resourcesToWrite =
      if (context.configuration.common.additionalOutputs.isEmpty) outputs
      else
        addAdditionalOutputFormats(
          outputs,
          context.configuration.common.additionalOutputs,
          context.configuration.common.outputFormat
        )

    val datasetNamesStr = outputs.keys.mkString("(", ", ", ")")
    logger.info(s"write datasets $datasetNamesStr")

    resourcesToWrite foreach { out =>
      logger.info(s"save dataset ${out._1}")
      writeTo(out._2)

      if (out._2.configuration.generateMetadata) {
        logger.info(s"save metadata for dataset ${out._1}")
        val metadata = metadataHelper(context).create(out._2)
        metadataHelper(context).write(metadata)
      }
    }

    outputs
  }

}
