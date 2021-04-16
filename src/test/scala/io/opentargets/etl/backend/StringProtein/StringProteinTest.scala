package io.opentargets.etl.backend.StringProtein

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.stringProtein._
import io.opentargets.etl.backend.stringProtein.StringProtein
import org.apache.spark.sql.functions.{array_contains, col, map_keys}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object StringProteinTest {

  def stringDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read
      .format("csv")
      .option("delimiter", " ")
      .option("header", true)
      .load(this.getClass.getResource("/9606.protein.links.test.v11.0.txt.gz").getPath)
}

class StringProteinTest extends EtlSparkUnitTest {
  import sparkSession.implicits._

  "Processing String input file " should "return a dataframe with a specific schema" in {
    // given
    val inputDF: DataFrame = StringProteinTest.stringDf(sparkSession)
    val expectedColumns = Set("interaction", "interactorA", "interactorB", "source_info")
    // when
    val results: DataFrame = StringProtein(inputDF, 0)(sparkSession)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

}
