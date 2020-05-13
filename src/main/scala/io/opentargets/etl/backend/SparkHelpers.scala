package io.opentargets.etl.backend
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SparkHelpers extends LazyLogging {
  type IOResourceConfs = Map[String, IOResourceConfig]
  type IOResources = Map[String, DataFrame]

  case class IOResourceConfig(format: String, path: String)

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
   Es. inputsDataFrame {"disease", Dataframe} , {"target", Dataframe}
   Reading is the first step in the pipeline
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
    session.read.format(pathInfo.format).load(pathInfo.path)
  }

  def writeTo(outputConfs: IOResourceConfs, outputs: IOResources)(
      implicit session: SparkSession): IOResources = {

    logger.info(s"Saving data to '${outputConfs.mkString(", ")}'")
    val dfs = outputs.toSeq.sortBy(_._1) zip outputConfs.toSeq.sortBy(_._1)

    dfs.foreach {
      case (df, conf) =>
        logger.debug(s"saving dataframe '${df._1}' into '${conf._2.path}'")
        df._2.write.format(conf._2.format).save(conf._2.path)
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

  def renameAllCols(schema: StructType, rename: String => String): StructType = {
    def recurRename(schema: StructType): Seq[StructField] = schema.fields.map {
      case StructField(name, dtype: StructType, nullable, meta) =>
        StructField(rename(name), StructType(recurRename(dtype)), nullable, meta)
      case StructField(name, dtype: ArrayType, nullable, meta)
          if dtype.elementType.isInstanceOf[StructType] =>
        StructField(
          rename(name),
          ArrayType(StructType(recurRename(dtype.elementType.asInstanceOf[StructType])),
                    containsNull = true),
          nullable,
          meta
        )
      case StructField(name, dtype, nullable, meta) =>
        StructField(rename(name), dtype, nullable, meta)
    }
    StructType(recurRename(schema))
  }
}
