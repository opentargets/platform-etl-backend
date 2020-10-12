package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.OTConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.util.Random

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
    * Create an IOResourceConf Map for each of the given files, where the file is a key and the value is the output
    * configuration
    * @param files will be the names out the output files
    * @param configuration to provide access to the program's configuration
    * @return a map of file -> IOResourceConfig
    */
  def generateDefaultIoOutputConfiguration(files: String*)(
      configuration: OTConfig): IOResourceConfs = {
    (for (n <- files)
      yield
        n -> IOResourceConfig(configuration.common.outputFormat,
                              configuration.common.output + s"/$n")) toMap

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
    logger.info(s"load file ${pathInfo.path} with format ${pathInfo.format} to dataframe")
    pathInfo match {
      // CSV, TSV, etc
      case IOResourceConfig(_: String,
                            format: String,
                            header: Option[String],
                            delimiter: Option[Boolean])
          if format.contains("sv") && header.isDefined && delimiter.isDefined =>
        logger.debug(
          s"Loading separated value file: header - ${pathInfo.header.get}, delimiter - ${pathInfo.delimiter.get}")
        session.read
          .format("csv")
          .option("header", pathInfo.header.get)
          .option("delimiter", pathInfo.delimiter.get)
          .load(pathInfo.path)

      case IOResourceConfig(_: String,
                            format: String,
                            header: Option[String],
                            delimiter: Option[Boolean]) if format.contains("sv") => {
        logger.error(
          s"Separated value filed ${pathInfo.path} selected without specifying header and/or delimiter values")
        // killing program through exception.
        assert(false, s"Unable to complete pipeline due to bad file configuration for $pathInfo")
        session.emptyDataFrame
      }
      // All other formats
      case _ => session.read.format(pathInfo.format).load(pathInfo.path)
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

  /**
    * Given a dataframe with a n columns, this method create a new column called `collectUnder` which will include all
    * columns listed in `includedColumns` in a struct column. Those columns will be removed from the original dataframe.
    * This can be used to nest fields.
    * @param dataFrame on which to perform nesting
    * @param includedColumns columns to include in new nested column
    * @param collectUnder name of new struct column
    * @return dataframe with new column `collectUnder` with `includedColumns` nested within it.
    */
  def nest(dataFrame: DataFrame, includedColumns: List[String], collectUnder: String): DataFrame = {
    // We need to use a random column name in case `collectUnder` is also in `includedColumns` as Spark SQL
    // isn't case sensitive.
    val tempCol: String = Random.alphanumeric.take(collectUnder.length + 2).mkString
    dataFrame
      .withColumn(tempCol, struct(includedColumns.map(col): _*))
      .drop(includedColumns: _*)
      .withColumnRenamed(tempCol, collectUnder)
  }

  /**
    * Helper function to confirm that all required columns are available on dataframe.
    * @param requiredColumns on input dataframe
    * @param dataFrame dataframe to test
    */
  def validateDF(requiredColumns: Set[String], dataFrame: DataFrame): Unit = {
    lazy val msg =
      s"One or more required columns (${requiredColumns.mkString(",")}) not found in dataFrame columns: ${dataFrame.columns
        .mkString(",")}"
    val columnsOnDf = dataFrame.columns.toSet
    assert(requiredColumns.forall(columnsOnDf.contains), msg)
  }
}
