package io.opentargets.etl.backend.Genetics

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.genetics.Gene

class GeneTest extends EtlSparkUnitTest {

  import sparkSession.implicits._

  "Only genes within the absolute distance of a variant" should "be returned" in {
    // given
    val variants = Seq((1, 1000)).toDF("chr_id", "position")
    val threeTargets = Seq((1, 100), (1, 700), (1, 1300)).toDF("chromosome", "tss")
    val distance = 500
    // when
    val df = Gene.variantGeneDistance(variants, distance)(threeTargets)
    // then
    assertResult(1, "One target within range.")(df.count())

  }
}
