package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class GeneticConstraintTest extends EtlSparkUnitTest {

  "The raw gnomad loss of function data set" should "be properly converted into genetic constraints" in {
    // given
    val input = sparkSession.read
      .option("sep", "\\t")
      .option("header", value = true)
      .csv(this.getClass.getResource("/target/gnomad_lof.csv").getPath)
    // when
    val results: Dataset[GeneticConstraintsWithId] = GeneticConstraints(input)(sparkSession)

    // then
    results.count() should be(50 - 1) // minus one for header.

  }
}
