package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TractabilityTest extends EtlSparkUnitTest {

  "The raw tractability dataset" should "be properly converted into TractabilityWithId instances" in {
    import sparkSession.implicits._
    // given
    val input = sparkSession.read
      .option("sep", "\\t")
      .option("header", value = true)
      .csv(this.getClass.getResource("/target/tractability_50.csv.gz").getPath)
    // when
    val results: Dataset[TractabilityWithId] = Tractability(input)(sparkSession)
    val tractabilityData = results
      .filter(_.ensemblGeneId.equals("ENSG00000128052"))
      .map(_.tractability)
      .collectAsList
      .get(0)

    // then
    results.count() should be(50)
    // check modality is as expected.
    tractabilityData.forall(trct => Set("SM", "OC", "PR", "AB").contains(trct.modality))

  }
}
