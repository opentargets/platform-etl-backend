package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResourceConfig, IOResourceConfigOption, IoHelpers}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.scalatest.prop.TableDrivenPropertyChecks
import io.opentargets.etl.backend.spark.Helpers._
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.util.Random

class HelpersTest extends EtlSparkUnitTest with TableDrivenPropertyChecks with LazyLogging {

  // given
  val renameFun: String => String = _.toUpperCase
  lazy val testStruct: StructType =
    StructType(
      StructField("a", IntegerType, nullable = true) ::
        StructField("b", LongType, nullable = false) ::
        StructField("c", BooleanType, nullable = false) :: Nil
    )
  lazy val testData: Seq[Row] = Seq(Row(1, 1L, true), Row(2, 2L, false))
  lazy val testDf: DataFrame =
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(testData), testStruct)

  "generateDefaultIoOutputConfiguration" should "generate a valid configuration for each of its input files" in {
    // given
    Configuration.config match {
      case Right(config) =>
        val inputFileNames = Seq("a", "b", "c")
        // when
        val results = IoHelpers.generateDefaultIoOutputConfiguration(inputFileNames: _*)(config)
        // then
        assert(results.keys.size == inputFileNames.size)
        assert(
          results.values.forall(ioResConf =>
            ioResConf.format == config.common.outputFormat &&
              inputFileNames.contains(ioResConf.path.split("/").last)
          )
        )

      case Left(ex) =>
        logger.error(ex.prettyPrint())
        assertDoesNotCompile("OT config loading problem")
    }
  }

  they should "load correctly when header and separator as specified" in {
    // given
    val path: String = this.getClass.getResource("/drugbank_v.csv").getPath
    val input = IOResourceConfig(
      "csv",
      path,
      Some(
        List(
          IOResourceConfigOption("sep", "\\t"),
          IOResourceConfigOption("header", "true")
        )
      )
    )
    // when
    val results = IoHelpers.loadFileToDF(input)(sparkSession)
    // then
    assert(!results.isEmpty, "The provided dataframe should not be empty.")
  }

  "Rename columns" should "rename all columns using given function" in {

    // when
    val results: StructType = renameAllCols(testStruct, renameFun)
    // then
    assert(results.fields.forall(sf => sf.name.head.isUpper))
  }

  it should "correctly rename columns in nested arrays" in {
    // given
    val structWithArray = testStruct
      .add(
        "d",
        ArrayType(
          new StructType()
            .add("e", StringType)
            .add("f", StringType)
            .add("g", IntegerType)
        )
      )
    // when
    val results = renameAllCols(structWithArray, renameFun)
    // then
    assert(
      results(3).dataType
        .asInstanceOf[ArrayType]
        .elementType
        .asInstanceOf[StructType]
        .fieldNames
        .forall(_.head.isUpper)
    )
  }

  private val potentialColumnNames = Table(
    "column names",
    "a", // shadow existing column name and case
    "d", // new name
    "A", // shadow existing column name, different case
    "_", // special characters
    "!",
    Random.alphanumeric.take(100).mkString
  ) // very long
  "nest" should "return dataframe with selected columns nested under new column" in {
    // given
    logger.debug(s"Input DF structure ${testDf.printSchema}")
    val columnsToNest = testDf.columns.toList
    forAll(potentialColumnNames) { (colName: String) =>
      // when
      val results = nest(testDf, columnsToNest, colName)
      logger.debug(s"Output DF schema: ${results.printSchema}")
      // then
      assertResult(1, s"All columns should be listed under new column $colName") {
        results.columns.length
      }
      assertResult(colName, "The nesting column should be appropriately named.") {
        results.columns.head
      }
    }

  }

  "safeArrayUnion" should "return elements of one array if the other is null" in {
    // given
    val json =
      """
        |{
        |  "cases": [
        |    { "id": 1, "a": [ 4, 5, 6], "c": [2]},
        |    { "id": 2, "a": [ 1, 2, 3], "b": [7]},
        |    { "id": 3, "a": [], "b": [], "c": []}
        |  ]
        |}
        |""".stripMargin
    import sparkSession.implicits._

    val df = sparkSession.read.json(Seq(json).toDS).select(explode(col("cases"))).select("col.*")
    // when
    val results = df.withColumn("d", safeArrayUnion(col("a"), col("b"), col("c")))

    // then
    results.filter($"d".isNotNull).count() should be(3)
    results
      .filter($"id" === 1)
      .select(functions.size($"d").as("union"))
      .head()
      .getAs[Int]("union") should be(4)

  }

  "unionDataframeDifferentSchema" should "return a dataframe which includes columns from df1 and df2 and all rows in each" in {
    // given
    val json1 =
      """
        |{
        |  "cases": [
        |    { "id": 1, "a": 4, "c": "str"},
        |    { "id": 2, "a": 5, "b": "str"},
        |    { "id": 3, "a": 11, "b": "str", "c": "str"}
        |  ]
        |}
        |""".stripMargin

    val json2 =
      """
        |{
        |  "cases": [
        |    { "id": 4, "d": 'd', "e": 2},
        |    { "id": 5, "a": 13, "e": 7},
        |    { "id": 6, "a": 1, "f": 'c', "c": "str"}
        |  ]
        |}
        |""".stripMargin
    import sparkSession.implicits._

    val df1 = sparkSession.read.json(Seq(json1).toDS).select(explode(col("cases"))).select("col.*")
    val df2 = sparkSession.read.json(Seq(json2).toDS).select(explode(col("cases"))).select("col.*")
    // when
    val result = unionDataframeDifferentSchema(df1, df2)
    // then
    val expectedOutputColumns = Seq("id", "a", "b", "c", "d", "e", "f")
    result.columns.length should be(expectedOutputColumns.length)
    result.columns sameElements expectedOutputColumns
  }

  "It" should "return the original data frame if it is merged with an empty dataframe" in {
    import sparkSession.implicits._
    // given
    val cols = Seq("a", "b", "c")
    val withColsDF = Seq.empty[(String, String, String)].toDF(cols: _*)
    val emptyDF = sparkSession.emptyDataFrame
    // when
    val result = unionDataframeDifferentSchema(emptyDF, withColsDF)
    // then
    result.columns sameElements cols
  }

}
