package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TepTest extends EtlSparkUnitTest {

  "Raw Tep file" should "be converted to dataset without loss" in {
    // given
    val df = sparkSession.read
      .json(this.getClass.getResource("/target/tep_test.json").getPath)
    // when
    val results = Tep(df)

    // then
    results.count() should be(df.select("targetFromSourceId").distinct.count())
  }

}
