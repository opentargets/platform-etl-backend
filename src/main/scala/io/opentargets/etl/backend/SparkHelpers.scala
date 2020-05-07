package io.opentargets.etl.backend
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SparkHelpers extends LazyLogging {

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
  def loader(
      inputFileConf: Map[String, Map[String, String]]
  )(implicit session: SparkSession): Map[String, DataFrame] = {
    logger.info("Load files into Hashmap Dataframe")
    for {
      (stepKey, stepFilename) <- inputFileConf
    } yield stepKey -> loadFileToDF(stepFilename)
  }

  def loadFileToDF(pathInfo: Map[String, String])(implicit session: SparkSession): DataFrame =
    session.read.format(pathInfo("format")).load(pathInfo("path"))

  def save(
      df: DataFrame,
      path: String,
      writerConfigurator: Option[WriterConfigurator] = None
  ): DataFrame = {
    writerConfigurator.getOrElse(defaultWriterConfigurator())(df.write).save(path)
    logger.info(s"Saved data to '$path'")
    df
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
