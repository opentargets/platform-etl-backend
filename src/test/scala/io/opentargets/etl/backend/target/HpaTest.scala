package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.HpaTest.getHpaDataframe
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object HpaTest {
  def getHpaDataframe(implicit sparkSession: SparkSession): DataFrame = {
    val path = this.getClass.getResource("/target/hpa_20.tsv").getPath
    sparkSession.read.option("sep", "\\t").option("header", value = true).csv(path)
  }
}

class HpaTest extends EtlSparkUnitTest {
  "Subcellular locations" should "be extracted from HPA raw data" in {
    import sparkSession.implicits._
    // given
    val inputs = getHpaDataframe
    // when
    val results: Dataset[HPA] = HPA(inputs)

    // then
    results.filter(_.id equals "ENSG00000001629").map(_.locations).head() should have size 2
    results.count() should be(19) // size of input file -1 because of headers.
  }
}
