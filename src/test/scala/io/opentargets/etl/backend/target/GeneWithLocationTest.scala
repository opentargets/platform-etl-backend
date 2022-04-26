package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.GeneWithLocationTest.getHpaDataframe
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object GeneWithLocationTest {
  def getHpaDataframe(implicit sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val pathHpa = this.getClass.getResource("/target/hpa_20.tsv").getPath
    val pathHpaSl = this.getClass.getResource("/target/hpa_sl.tsv").getPath
    val reader: String => DataFrame =
      sparkSession.read.option("sep", "\\t").option("header", value = true).csv(_)
    (reader(pathHpa), reader(pathHpaSl))
  }
}

class GeneWithLocationTest extends EtlSparkUnitTest {
  "Subcellular locations" should "be extracted from HPA raw data" in {
    import sparkSession.implicits._
    // given
    val (hpa, sl) = getHpaDataframe
    // when
    val results: Dataset[GeneWithLocation] = GeneWithLocation(hpa, sl)

    // then
    results.filter(_.id equals "ENSG00000001629").map(_.locations).head() should have size 2
    results.count() should be(19) // size of input file -1 because of headers.
  }
}
