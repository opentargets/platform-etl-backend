package io.opentargets.etl.backend.StringProtein

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.stringProtein.StringProtein
import org.apache.spark.sql.DataFrame

object StringProteinTest {

  def stringDf(implicit context: ETLSessionContext): DataFrame = {
    val sparkSession = context.sparkSession
    sparkSession.read
      .format("csv")
      .option("delimiter", " ")
      .option("header", true)
      .load(this.getClass.getResource("/9606.protein.links.test.v11.0.txt.gz").getPath)
  }
}

class StringProteinTest extends EtlSparkUnitTest {

  "Processing String input file " should "return a dataframe with a specific schema" in {

    // given
    val inputDF: DataFrame = StringProteinTest.stringDf(ctx)
    val expectedColumns = Set("interaction", "interactorA", "interactorB", "source_info")
    // when
    val results: DataFrame = StringProtein(inputDF, 0)(ctx)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

}
