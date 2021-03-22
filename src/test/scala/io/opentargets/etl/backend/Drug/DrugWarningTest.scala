package io.opentargets.etl.backend.Drug

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.drug.DrugWarning
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DrugWarningTest extends EtlSparkUnitTest {
  "Drug warnings" should "be annotated with Meddra SOC codes" in {
    // given
    val inputDf =
      sparkSession.read.json(this.getClass.getResource("/drug_warning_30.jsonl").getPath)
    // when
    val results = DrugWarning(inputDf)

    // then
    results.count should be(30)
    results.filter(col("toxicityClass").isNotNull && col("meddraSocCode").isNull).count should be(0)
  }
}
