package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.OTConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.language.postfixOps
import scala.util.Random

object Helpers extends LazyLogging {
  type IOResourceConfs = Map[String, IOResourceConfig]
  type IOResources = Map[String, DataFrame]

  case class IOResourceConfigOption(k: String, v: String)
  case class IOResourceConfig(
      format: String,
      path: String,
      options: Option[Seq[IOResourceConfigOption]] = None,
      partitionBy: Option[Seq[String]] = None
  )

  /** generate a spark session given the arguments if sparkUri is None then try to get from env
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

  /** Create an IOResourceConf Map for each of the given files, where the file is a key and the value is the output
    * configuration
    * @param files will be the names out the output files
    * @param configuration to provide access to the program's configuration
    * @return a map of file -> IOResourceConfig
    */
  def generateDefaultIoOutputConfiguration(
      files: String*
  )(configuration: OTConfig): IOResourceConfs = {
    files.map {
      n => n -> IOResourceConfig(
        configuration.common.outputFormat,
        configuration.common.output + s"/$n")
    } toMap
  }

  /** apply to newNameFn() to the new name for the transformation and columnFn() to the inColumn
    * it returns a pair that can be used to create a map of transformations. Useful to use with
    * withColumn DataFrame function too
    */
  def trans(inColumn: Column,
            newNameFn: String => String,
            columnFn: Column => Column): (String, Column) = {

    val name = newNameFn(inColumn.toString)
    val oper = columnFn(inColumn)

    logger.info(s"tranform ${oper.toString} -> $name")
    name -> oper
  }

  /** using the uri get the last token as an ID by example
    * http://identifiers.org/chembl.compound/CHEMBL207538 -> CHEMBL207538
    * */
  def stripIDFromURI(uri: Column): Column =
    substring_index(uri, "/", -1)

  def mkFlattenArray(col: Column, cols: Column*): Column = {
    val colss = col +: cols
    val colV = array(colss: _*)

    filter(
      array_distinct(
        flatten(
          filter(colV, x => x.isNotNull)
        )
      ),
      z => z.isNotNull
    )
  }

  /** colNames are columns to flat if any inner array and then concatenate them
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
    logger.info(s"load dataset ${pathInfo.path} with ${pathInfo.toString}")

    pathInfo.options.foldLeft(session.read.format(pathInfo.format)) {
      case ops =>
        val options = ops._2.map(c => c.k -> c.v).toMap
        ops._1.options(options)
    }.load(pathInfo.path)
  }

  /**
    * Helper function to prepare multiple files of the same category to be read by `readFrom`
    * @param resourceConfigs collection of IOResourceConfig of unknown composition
    * @return Map with random keys to input resource.
    */
  def seqToIOResourceConfigMap(resourceConfigs: Seq[IOResourceConfig]): IOResourceConfs = {
    (for (rc <- resourceConfigs) yield Random.alphanumeric.take(6).toString -> rc).toMap
  }

  def writeTo(outputConfs: IOResourceConfs, outputs: IOResources)(
      implicit
      session: SparkSession): IOResources = {
    outputConfs foreach {
      c => logger.info(s"save dataset ${c._1} with ${c._2.toString}")
    }

    // WARNING: SIDE-EFFECTING
    val outputsSaved = outputConfs.keySet intersect outputs.keySet map {
      case k =>
        val conf = outputConfs(k)
        val df = outputs(k)
        logger.debug(s"saving dataframe '$k' into '${conf.path}'")

        val pb = conf.partitionBy.foldLeft(df.write){
          case ops =>
            logger.debug(s"enabled partition by ${ops._2.toString}")
            ops._1.partitionBy(ops._2:_*)
        }

        conf.options.foldLeft(pb) {
          case ops =>
            logger.debug(s"write to ${conf.path} with options ${ops._2.toString}")
            val options = ops._2.map(c => c.k -> c.v).toMap
            ops._1.options(options)

        }.format(conf.format)
          .save(conf.path)

        k -> df
    } toMap

    val savedKeys = outputs.keySet intersect outputsSaved.keySet
    val allKeys = outputs.keySet union outputsSaved.keySet
    (allKeys diff savedKeys).foreach {
      k => logger.warn(s"dataframe $k has not been saved")
    }

    outputsSaved
  }

  /** generate a set of String with the union of Columns.
    * Eg, myCols =( a,c,d) and allCols(a,c,d,e,f,h)
    * return (a,c,d,e,f,h)
    * @param myCols the list of the Columns in a specific Dataframe
    * @param allCols the list of Columns to match
    * @return a sparksession object
    */
  def columnExpr(myCols: Set[String], allCols: Set[String]): Set[Column] = {
    val inter = (allCols intersect myCols).map(col)
    val differ = (allCols diff myCols).map(lit(null).as(_))

    inter union differ
  }

  /** generate the union between two dataframe with different Schema.
    * df is the implicit dataframe
    * @param df2 Dataframe with possibly a different Columns
    * @return a DataFrame
    */
  def unionDataframeDifferentSchema(df: DataFrame, df2: DataFrame): DataFrame = {
    val cols1 = df.columns.toSet
    val cols2 = df2.columns.toSet
    val total = cols1 ++ cols2 // union

    // Union between two dataframes with different schema. columnExpr helps to unify the schema
    val unionDF =
      df.select(columnExpr(cols1, total).toList: _*)
        .unionByName(df2.select(columnExpr(cols2, total).toList: _*))
    unionDF
  }

  /** generate snake to camel for the Elasticsearch indices.
    * Replace all _ with Capiltal letter except the first letter. Eg. "abc_def_gh" => "abcDefGh"
    * @param df Dataframe
    * @return a DataFrame with the schema lowerCamel
    */
  def snakeToLowerCamelSchema(df: DataFrame)(implicit session: SparkSession): DataFrame = {

    //replace all _ with Capiltal letter except the first letter. Eg. "abc_def_gh" => "abcDefGh"
    val snakeToLowerCamelFnc = (s: String) => {
      val tokens = s.split("_")
      tokens.head + tokens.tail.map(_.capitalize).mkString
    }

    val newDF =
      session.createDataFrame(df.rdd, renameAllCols(df.schema, snakeToLowerCamelFnc))

    newDF
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

  /** Given a dataframe with a n columns, this method create a new column called `collectUnder` which will include all
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

  /** Helper function to confirm that all required columns are available on dataframe.
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
