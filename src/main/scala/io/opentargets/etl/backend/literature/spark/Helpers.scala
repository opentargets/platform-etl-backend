package io.opentargets.etl.backend.literature.spark

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.{LiteratureModelConfiguration, OTConfig}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors.norm
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Random

object Helpers extends LazyLogging {

  /** generate a spark session given the arguments if sparkUri is None then try to get from env
    * otherwise it will set the master explicitly
    *
    * @param appName
    *   the app name
    * @param sparkUri
    *   uri for the spark env master if None then it will try to get from yarn
    * @return
    *   a sparksession object
    */
  def getOrCreateSparkSession(appName: String, sparkUri: Option[String]): SparkSession = {
    logger.info(s"create spark session with uri:'${sparkUri.toString}'")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.driver.maxResultSize", "0")
      .set("spark.debug.maxToStringFields", "2000")
      .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

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

  def makeWord2VecModel(
      df: DataFrame,
      modelConfiguration: LiteratureModelConfiguration,
      inputColName: String,
      outputColName: String = "prediction"
  ): Word2VecModel = {
    logger.info(s"compute Word2Vec model for input col $inputColName into $outputColName")

    val w2vModel = new Word2Vec()
      .setWindowSize(modelConfiguration.windowSize)
      .setNumPartitions(modelConfiguration.numPartitions)
      .setMaxIter(modelConfiguration.maxIter)
      .setMinCount(modelConfiguration.minCount)
      .setStepSize(modelConfiguration.stepSize)
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    val model = w2vModel.fit(df)

    model
  }

  def computeSimilarityScore(col1: Column, col2: Column): Column = {
    val cossim = udf { (v1: Vector, v2: Vector) =>
      val n1 = norm(v1, 2d)
      val n2 = norm(v2, 2d)
      val denom = n1 * n2
      if (denom == 0.0) 0.0
      else (v1 dot v2) / denom
    }

    cossim(col1, col2)
  }

  def normalise(c: Column): Column =
    // https://www.rapidtables.com/math/symbols/greek_alphabet.html
    translate(c, "αβγδεζηικλμνξπτυω", "abgdezhiklmnxptuo")

  def harmonicFn(c: Column): Column =
    aggregate(
      zip_with(sort_array(c, asc = false), sequence(lit(1), size(c)), (e1, e2) => e1 / pow(e2, 2d)),
      lit(0d),
      (c1, c2) => c1 + c2
    )

  def renameAllCols(schema: StructType, fn: String => String): StructType = {

    def renameDataType(dt: StructType): StructType =
      StructType(dt.fields.map { case StructField(name, dataType, nullable, metadata) =>
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

  /** generate snake to camel for the Elasticsearch indices. Replace all _ with Capiltal letter
    * except the first letter. Eg. "abc_def_gh" => "abcDefGh"
    *
    * @param df
    *   Dataframe
    * @return
    *   a DataFrame with the schema lowerCamel
    */
  def snakeToLowerCamelSchema(df: DataFrame)(implicit session: SparkSession): DataFrame = {

    // replace all _ with Capiltal letter except the first letter. Eg. "abc_def_gh" => "abcDefGh"
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

    // replace all spaces with _
    val renameFcn = (s: String) => s.replaceAll(" ", "_")

    val newDF =
      session.createDataFrame(df.rdd, renameAllCols(df.schema, renameFcn))

    newDF
  }

}
