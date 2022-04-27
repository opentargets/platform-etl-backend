package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.spark.Helpers.{idAndSourceSchema, labelAndSourceSchema}
import io.opentargets.etl.backend.target.TargetUtils._
import org.apache.spark.sql.functions.{col, explode, typedLit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TargetUtilsTest extends EtlSparkUnitTest {

  "TargetUtils" should "transform id column to label and source struct" in {
    //root
    // |-- ensemblId: string (nullable = true)
    // |-- hgncSynonyms: array (nullable = true)
    // |    |-- element: string (containsNull = true)
    //
    //root
    // |-- ensemblId: string (nullable = true)
    // |-- hgncSynonyms: array (nullable = true)
    // |    |-- element: string (containsNull = true)
    // |-- hgncSynonymsLabel: array (nullable = true)
    // |    |-- element: struct (containsNull = true)
    // |    |    |-- label: string (nullable = true)
    // |    |    |-- source: string (nullable = true)
    // |-- hgncSynonymsId: array (nullable = true)
    // |    |-- element: struct (containsNull = true)
    // |    |    |-- id: string (nullable = true)
    // |    |    |-- source: string (nullable = true)
    //
    //+---------+---------------+---------------------------------------+---------------------------------------+
    //|ensemblId|hgncSynonyms   |hgncSynonymsLabel                      |hgncSynonymsId                         |
    //+---------+---------------+---------------------------------------+---------------------------------------+
    //|ENSG1    |[foo, baz, bar]|[{foo, HGNC}, {baz, HGNC}, {bar, HGNC}]|[{foo, HGNC}, {baz, HGNC}, {bar, HGNC}]|
    //|ENSG2    |[foo, baz, bar]|[{foo, HGNC}, {baz, HGNC}, {bar, HGNC}]|[{foo, HGNC}, {baz, HGNC}, {bar, HGNC}]|
    //|ENSG3    |[foo, baz, bar]|[{foo, HGNC}, {baz, HGNC}, {bar, HGNC}]|[{foo, HGNC}, {baz, HGNC}, {bar, HGNC}]|
    //|ENSG4    |null           |null                                   |null                                   |
    //|ENSG5    |[]             |[]                                     |[]                                     |
    //+---------+---------------+---------------------------------------+---------------------------------------+

    import sparkSession.implicits._
    val columns = Seq("ensemblId", "hgncSynonyms")
    val data = Seq(
      ("ENSG1", Some(Array("foo", "baz", "bar"))),
      ("ENSG2", Some(Array("foo", "baz", "bar"))),
      ("ENSG3", Some(Array("foo", "baz", "bar"))),
      ("ENSG4", None),
      ("ENSG5", Some(Array.empty[String]))
    )
    val df =
      sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data)).toDF(columns: _*)

    val res =
      df.withColumn(
        "hgncSynonymsLabel",
        transformArrayToStruct(col("hgncSynonyms"), typedLit("HGNC") :: Nil, labelAndSourceSchema)
      ).withColumn(
        "hgncSynonymsId",
        transformArrayToStruct(col("hgncSynonyms"), typedLit("HGNC") :: Nil, idAndSourceSchema)
      )

    res.columns should contain("hgncSynonymsLabel").and(contain("hgncSynonymsId"))

    res
      .select(explode(col("hgncSynonymsLabel.source")).as("source"))
      .filter(col("source") =!= "HGNC")
      .count should be(0)
  }
}
