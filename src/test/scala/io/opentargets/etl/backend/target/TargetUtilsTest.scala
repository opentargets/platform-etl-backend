package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TargetUtilsTest extends EtlSparkUnitTest {

  "TargetUtils" should "transform id column to label and source struct" in {
    import sparkSession.implicits._
    val columns = Seq("ensemblId", "hgncSynonyms")
    val data = Seq(("ENSG1", Array("foo", "baz", "bar")),
                   ("ENSG2", Array("foo", "baz", "bar")),
                   ("ENSG3", Array("foo", "baz", "bar")))
    val df =
      sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data)).toDF(columns: _*)

    val res =
      TargetUtils.transformColumnToLabelAndSourceStruct(df, "ensemblId", "hgncSynonyms", "HGNC")
    res.columns should contain("hgncSynonyms")
    res
      .select(explode(col("hgncSynonyms.source")).as("source"))
      .filter(col("source") =!= "HGNC")
      .count should be(0)
  }
}
