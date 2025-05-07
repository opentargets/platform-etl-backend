package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

/** Options to be used by Spark to configure dataframe loading. */
case class IOResourceConfigOption(k: String, v: String)

/** Combines data and metadata regarding storage/retrieval */
case class IOResource(data: DataFrame, configuration: IOResourceConfig)
case class IOResourceML(data: Word2VecModel, configuration: IOResourceConfig)

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
  */
case class IOResourceConfig(
    format: String,
    path: String,
    options: Option[Seq[IOResourceConfigOption]] = None,
    partitionBy: Option[Seq[String]] = None
)

object CsvHelpers {
  val tsvWithHeader: Option[Seq[IOResourceConfigOption]] = Some(
    Seq(IOResourceConfigOption("header", "true"), IOResourceConfigOption("sep", "\t"))
  )
}

object IoHelpers extends LazyLogging {
  type IOResourceConfigurations = Map[String, IOResourceConfig]
  type IOResources = Map[String, IOResource]

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

    val effectivePath = if (pathInfo.path.endsWith(".zip")) {
      extractFileFromZip(pathInfo.path)
    } else {
      pathInfo.path
    }

    try
      pathInfo.options
        .foldLeft(session.read.format(pathInfo.format)) { case (reader, options) =>
          reader.options(options.map(c => c.k -> c.v).toMap)
        }
        .load(effectivePath)
    catch {
      case e: Exception =>
        logger.error(s"Error loading file $effectivePath with ${pathInfo.toString}")
        throw e
    }
  }

  private def extractFileFromZip(zipPath: String): String = {
    import better.files._

    val zipFile = File(zipPath)
    val innerFileName = zipFile.nameWithoutExtension
    val tempDir = File.newTemporaryDirectory("spark_zip_extract_")

    logger.info(s"Extracting file $innerFileName from zip $zipPath to $tempDir")

    try {
      zipFile.unzipTo(tempDir)

      val extractedFiles = tempDir.children.toList
      val matchingFile = extractedFiles.find(f => f.name == innerFileName)

      matchingFile match {
        case Some(file) =>
          logger.info(s"Found matching file: ${file.pathAsString}")
          file.pathAsString
        case None =>
          extractedFiles.headOption match {
            case Some(file) =>
              logger.info(s"No exact match found, using first file: ${file.pathAsString}")
              file.pathAsString
            case None =>
              logger.error(s"No files found in zip archive: $zipPath")
              throw new RuntimeException(s"Empty zip archive: $zipPath")
          }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error extracting from zip file $zipPath", e)
        tempDir.delete(swallowIOExceptions = true)
        throw e
    }
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

  // TODO: Refactorizar para recibir config
  def writeTo(output: IOResourceML)(implicit context: ETLSessionContext): IOResourceML = {
    val configuration = output.configuration

    val outputPath = configuration.path

    context.configuration.sparkSettings.writeMode match {
      case "overwrite" => output.data.write.overwrite().save(outputPath)
      case _           => output.data.save(outputPath)
    }

    output
  }

  private def writeTo(output: IOResource)(implicit context: ETLSessionContext): IOResource = {
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

    resourcesToWrite.values.foreach(writeTo)

    outputs
  }

}
