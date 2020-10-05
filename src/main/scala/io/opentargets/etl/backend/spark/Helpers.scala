package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql._

object Helpers extends LazyLogging {
  type IOResourceConfs = Map[String, IOResourceConfig]
  type IOResources = Map[String, DataFrame]

  case class IOResourceConfig(
      format: String,
      path: String,
      delimiter: Option[String] = None,
      header: Option[Boolean] = None
  )

  /**
    * generate a spark session given the arguments if sparkUri is None then try to get from env
    * otherwise it will set the master explicitely
    * @param appName the app name
    * @param sparkUri uri for the spark env master if None then it will try to get from yarn
    * @return a sparksession object
    */
  def getOrCreateSparkSession(appName: String, sparkUri: Option[String]): SparkSession = {
    logger.info(s"create spark session with uri:'${sparkUri.toString}'")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.driver.maxResultSize", "0")
      .set("spark.debug.maxToStringFields", "2000")

    // if some uri then setmaster must be set otherwise
    // it tries to get from env if any yarn running
    val conf = sparkUri match {
      case Some(uri) if uri.nonEmpty => sparkConf.setMaster(uri)
      case _                         => sparkConf
    }

    SparkSession.builder
      .config(conf)
      .getOrCreate
  }

  /**
    * colNames are columns to flat if any inner array and then concatenate them
    * @param colNames list of column names as string
    * @return A `Column` ready to be used as any other column operator
    */
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

  type WriterConfigurator = DataFrameWriter[Row] => DataFrameWriter[Row]

  // Return sensible defaults, possibly modified by configuration if necessary in the future. Eg. parquet
  private def defaultWriterConfigurator(): WriterConfigurator =
    (writer: DataFrameWriter[Row]) => writer.format("json").mode("overwrite")

  /** It creates an hashmap of dataframes.
    *   Es. inputsDataFrame {"disease", Dataframe} , {"target", Dataframe}
    *   Reading is the first step in the pipeline
    */
  def readFrom(
      inputFileConf: IOResourceConfs
  )(implicit session: SparkSession): IOResources = {
    logger.info("Load files into Hashmap Dataframe")
    for {
      (key, formatAndPath) <- inputFileConf
    } yield key -> loadFileToDF(formatAndPath)
  }

  def loadFileToDF(pathInfo: IOResourceConfig)(implicit session: SparkSession): DataFrame = {
    logger.debug(s"load file ${pathInfo.path} with format ${pathInfo.format} to dataframe")
    if (pathInfo.format.contains("sv")) {
      logger.debug("some ice")
      session.read
        .format("csv")
        .option("header", pathInfo.header.get)
        .option("delimiter", pathInfo.delimiter.get)
        .load(pathInfo.path)
    } else {
      session.read.format(pathInfo.format).load(pathInfo.path)
    }
  }

  def writeTo(outputConfs: IOResourceConfs, outputs: IOResources)(
      implicit
      session: SparkSession): IOResources = {

    logger.info(s"Saving data to '${outputConfs.mkString(", ")}'")

    outputConfs foreach {
      case (n, c) =>
        logger.debug(s"saving dataframe '$n' into '${c.path}'")
        outputs(n).write.format(c.format).save(c.path)
    }

    outputs
  }

  // Replace the spaces from the schema fields with _
  def replaceSpacesSchema(df: DataFrame)(implicit session: SparkSession): DataFrame = {

    //replace all spaces with _
    val renameFcn = (s: String) => s.replaceAll(" ", "_")

    val newDF =
      session.createDataFrame(df.rdd, renameAllCols(df.schema, renameFcn))

    newDF
  }

  def renameAllCols(schema: StructType, fn: String => String): StructType = {

    def renameDataType(dt: StructType): StructType =
      StructType(dt.fields.map {
        case StructField(name, dataType, nullable, metadata) =>
          val renamedDT = dataType match {
            case st: StructType => renameDataType(st)
            case ArrayType(elementType: StructType, containsNull) =>
              ArrayType(renameDataType(elementType), containsNull)
            case rest: DataType => rest
          }
          StructField(fn(name), renamedDT, nullable, metadata)
      })

    renameDataType(schema)
  }
}
