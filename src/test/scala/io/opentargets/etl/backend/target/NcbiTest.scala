package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class NcbiTest extends EtlSparkUnitTest {
  "Raw ncbi entrez inputs" should "be converted to Ncbi case class without error" in {
    // given
    val df = sparkSession.read
      .option("header", true)
      .option("sep", "\\t")
      .csv(this.getClass.getResource("/target/ncbi_100.tsv.gz").getPath)

    // when
    val results = Ncbi(df)

    // then
    results.count should be > 0L
  }
}
