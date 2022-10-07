package io.opentargets.etl.backend.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{
  aggregate,
  array,
  array_distinct,
  array_union,
  coalesce,
  col,
  explode,
  expr,
  filter,
  flatten,
  lit,
  pow,
  sequence,
  size,
  sort_array,
  struct,
  substring_index,
  typedLit,
  zip_with
}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.language.postfixOps
import scala.util.Random

object Helpers extends LazyLogging {

  /** generate a string prefix with `length` characters and ends like 'abcd_' where '_' is added at
    * the end automatically
    * @param length
    *   the number of random characters to build the string prefix default to 5
    * @return
    *   the string suffixed with underscore
    */
  def mkRandomPrefix(length: Int = 5): String =
    Random.alphanumeric.take(length).mkString("", "", "_")
  case class IdAndSource(id: String, source: String)
  case class LabelAndSource(label: String, source: String)

  case class LocationAndSource(location: String, source: String, termSl: String, labelSl: String)

  val idAndSourceSchema: StructType = Encoders.product[IdAndSource].schema
  val labelAndSourceSchema: StructType = Encoders.product[LabelAndSource].schema
  val locationAndSourceSchema: StructType = Encoders.product[LocationAndSource].schema

  /** Returns input string wrapped in backticks if it contains period character.
    *
    * Spark interprets the . symbol to be a select. Input files may include this in their column
    * names causing unanticipated behaviour.
    */
  val wrapColumnNamesWithPeriodCharacters: String => String = {
    case a if a.contains(".") => s"`$a`"
    case s                    => s
  }

  /** generate a spark session given the arguments if sparkUri is None then try to get from env
    * otherwise it will set the master explicitely
    *
    * @param appName
    *   the app name
    * @param sparkUri
    *   uri for the spark env master if None then it will try to get from yarn
    * @return
    *   a sparksession object
    */
  def getOrCreateSparkSession(appName: String,
                              configKeys: Seq[IOResourceConfigOption],
                              sparkUri: Option[String]
  ): SparkSession = {

    val conf = getSparkSessionConfig(appName, configKeys, sparkUri)

    SparkSession.builder
      .config(conf)
      .getOrCreate
  }

  def getSparkSessionConfig(appName: String,
                            configKeys: Seq[IOResourceConfigOption],
                            sparkUri: Option[String]
  ): SparkConf = {
    logger.info(s"create spark session with uri:'${sparkUri.toString}'")
    val keys = configKeys.map(va => (va.k, va.v))
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(appName)
      .setAll(keys)

    // if some uri then setmaster must be set otherwise
    // it tries to get from env if any yarn running
    val conf = sparkUri match {
      case Some(uri) if uri.nonEmpty => sparkConf.setMaster(uri)
      case _                         => sparkConf
    }

    conf
  }

  /** apply to newNameFn() to the new name for the transformation and columnFn() to the inColumn it
    * returns a pair that can be used to create a map of transformations. Useful to use with
    * withColumn DataFrame function too
    */
  def trans(
      inColumn: Column,
      newNameFn: String => String,
      columnFn: Column => Column
  ): (String, Column) = {

    val name = newNameFn(inColumn.toString)
    val oper = columnFn(inColumn)

    logger.info(s"tranform ${oper.toString} -> $name")
    name -> oper
  }

  /** using the uri get the last token as an ID by example
    * http://identifiers.org/chembl.compound/CHEMBL207538 -> CHEMBL207538
    */
  def stripIDFromURI(uri: Column): Column =
    substring_index(uri, "/", -1)

  /** @param col
    *   Column of array type
    * @param cols
    *   Columns of array type
    * @return
    *   column of array type with input columns combined into single array with duplicates removed.
    */
  def mkFlattenArray(col: Column, cols: Column*): Column = {
    val colss: Seq[Column] = col +: cols
    val colV: Column = array(colss: _*)

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
    * @param colNames
    *   list of column names as string
    * @return
    *   A `Column` ready to be used as any other column operator
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

  /** Transpose a Dataframe column to row df is the implicit dataframe
    * | ID    | abdomen | aorta | col_... |
    * |:------|:--------|:------|:--------|
    * | ENSG1 | 0.0     | 0.6   | ...     |
    * | ENSG2 | 0.5     | 0.7   | ...     |
    * to
    * | ID    | key     | val  |
    * |:------|:--------|:-----|
    * | ENSG1 | abdomen | 0.00 |
    * | ENSG1 | aorta   | 0.6  |
    * | ENSG2 | abdomen | 0.5  |
    * | ENSG2 | aorta   | 0.7  |
    * @param by
    *   Column name pivot
    * @return
    *   a DataFrame
    */
  def transposeDataframe(df: DataFrame, by: Seq[String]): DataFrame = {
    val (cols, types) = df.dtypes.filter { case (c, _) => !by.contains(c) }.unzip
    // require(types.distinct.size == 1, s"${types.distinct.toString}.length != 1")

    val kvs = explode(
      array(
        cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*
      )
    )

    val byExprs = by.map(col)

    df.select(byExprs :+ kvs.alias("_kvs"): _*)
      .select(byExprs ++ Seq(col("_kvs.key"), col("_kvs.val")): _*)
  }

  /** generate the union between two dataframe with different Schema. df is the implicit dataframe
    *
    * @param df2
    *   Dataframe with possibly a different Columns
    * @return
    *   a DataFrame
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

  def unionDataframeDifferentSchema(df: Seq[DataFrame]): DataFrame =
    df.reduce((a, b) => unionDataframeDifferentSchema(a, b))

  /** generate a set of String with the union of Columns. Eg, myCols =( a,c,d) and
    * allCols(a,c,d,e,f,h) return (a,c,d,e,f,h)
    *
    * @param myCols
    *   the list of the Columns in a specific Dataframe
    * @param allCols
    *   the list of Columns to match
    * @return
    *   a sparksession object
    */
  def columnExpr(myCols: Set[String], allCols: Set[String]): Set[Column] = {
    val inter = (allCols intersect myCols).map(col)
    val differ = (allCols diff myCols).map(lit(null).as(_))

    inter union differ
  }

  /** generate snake to camel for the Elasticsearch indices. Replace all _ with Capiltal letter
    * except the first letter. Eg. "abc_def_gh" => "abcDefGh"
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

  // Replace the spaces from the schema fields with _
  def replaceSpacesSchema(df: DataFrame)(implicit session: SparkSession): DataFrame = {

    // replace all spaces with _
    val renameFcn = (s: String) => s.replaceAll(" ", "_")

    val newDF =
      session.createDataFrame(df.rdd, renameAllCols(df.schema, renameFcn))

    newDF
  }

  /** Given a dataframe with a n columns, this method create a new column called `collectUnder`
    * which will include all columns listed in `includedColumns` in a struct column. Those columns
    * will be removed from the original dataframe. This can be used to nest fields.
    *
    * @param dataFrame
    *   on which to perform nesting
    * @param includedColumns
    *   columns to include in new nested column
    * @param collectUnder
    *   name of new struct column
    * @return
    *   dataframe with new column `collectUnder` with `includedColumns` nested within it.
    */
  def nest(dataFrame: DataFrame, includedColumns: List[String], collectUnder: String): DataFrame = {
    // We need to use a random column name in case `collectUnder` is also in `includedColumns` as Spark SQL
    // isn't case sensitive.
    val tempCol: String = scala.util.Random.alphanumeric.take(collectUnder.length + 2).mkString
    dataFrame
      .withColumn(tempCol, struct(includedColumns.map(col): _*))
      .drop(includedColumns: _*)
      .withColumnRenamed(tempCol, collectUnder)
  }

  /** Helper function to confirm that all required columns are available on dataframe.
    *
    * @param requiredColumns
    *   on input dataframe
    * @param dataFrame
    *   dataframe to test
    */
  def validateDF(requiredColumns: Set[String], dataFrame: DataFrame): Unit = {
    lazy val msg =
      s"One or more required columns (${requiredColumns.mkString(",")}) not found in dataFrame columns: ${dataFrame.columns
          .mkString(",")}"
    val columnsOnDf = dataFrame.columns.toSet
    assert(requiredColumns.forall(columnsOnDf.contains), msg)
  }

  /** Returns the result of array_union(arr_1, ..., arr_n) where null arrays are cast to empty.
    *
    * The default implementation of array_union returns null if any of the input arrays is null.
    * This method meets the need of joining arrays where one or more of them may be null, but we
    * still want the partial result returned.
    *
    * @param columns
    *   of array type
    * @return
    *   union of columns
    */
  def safeArrayUnion(columns: Column*): Column =
    columns.map(coalesce(_, typedLit(Array.empty))).reduce((c1, c2) => array_union(c1, c2))

}
