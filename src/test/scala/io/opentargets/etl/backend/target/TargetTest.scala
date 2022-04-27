package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object TargetTest {}

class TargetTest extends EtlSparkUnitTest {

  "ProteinIds" should "not have duplicate accession entries and be returned in sorted order" in {
    // given
    val input = Array(
      "ENSP00000252338-ensembl_PRO",
      "O75949-uniprot_swissprot",
      "O75949-uniprot",
      "B1ALV6-uniprot",
      "D3DVU1-uniprot"
    )
    // when
    val results = Target.cleanProteinIds(input)

    // then
    results should contain inOrder ("O75949-uniprot_swissprot", "B1ALV6-uniprot", "D3DVU1-uniprot", "ENSP00000252338-ensembl_PRO")
  }
}
