package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.HallmarksTest.hallmarksRawDf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HallmarksTest {
  def hallmarksRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read
      .options(Map("header" -> "true", "sep" -> "\\t"))
      .csv(this.getClass.getResource("/target/cosmic-hallmarks-2021-02-10.tsv.gz").getPath)
}

class HallmarksTest extends EtlSparkUnitTest {

  "Hallmarks" should "correctly transform raw data into a typed Dataset and include each gene symbol" in {
    // given
    val rawDF = hallmarksRawDf
    // when
    val ds: Dataset[HallmarksWithId] = Hallmarks(rawDF)
    // then
    ds.select("approvedSymbol").distinct.count must equal(
      rawDF.select("GENE_SYMBOL").distinct.count
    )
  }

  lazy val ds: Dataset[HallmarksWithId] = Hallmarks(hallmarksRawDf).cache()
  "Cancer hallmarks" should "only include 10 distinct annotation types" in {
    // given
    // then
    ds.select(explode(col("hallmarks.cancerHallmarks")) as "ch")
      .select("ch.label")
      .distinct
      .count must equal(10)
  }

  "Attributes" should "only include 12 distinct annotation types" in {
    // given
    // then
    ds.select(explode(col("hallmarks.attributes")) as "a")
      .select("a.attribute_name")
      .distinct
      .count must equal(12)
  }
}
