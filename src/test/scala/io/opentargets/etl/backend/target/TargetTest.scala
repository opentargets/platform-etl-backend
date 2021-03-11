package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.Configuration.OTConfig
import io.opentargets.etl.backend.{Configuration, EtlSparkUnitTest}
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig}
import io.opentargets.etl.backend.target.UniprotTest.uniprotDataPath
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import pureconfig.ConfigReader

object TargetTest {}

class TargetTest extends EtlSparkUnitTest {
  "Target" should "read in raw Uniprot data and preprocessing complete" in {
//    // given
//    val conf: ConfigReader.Result[OTConfig] = Configuration.config
//    val getUniprotDataFrame = PrivateMethod[Map[String, IOResource]]('getMappedInputs)
    //    // when
    //    val results = io.opentargets.etl.backend.target.Target invokePrivate getUniprotDataFrame(
    //      conf.right.get.target,
    //      sparkSession)
    //    // then
    //    val uniprot = results.get("uniprot")
    //    uniprot shouldBe defined
    //    uniprot.get.data.count() must be(10)
  }

  "ProteinIds" should "not have duplicate accession entries and be returned in sorted order" in {
    // given
    val input = Array("ENSP00000252338-ensembl_PRO",
                      "O75949-uniprot_swissprot",
                      "O75949-uniprot",
                      "B1ALV6-uniprot",
                      "D3DVU1-uniprot")
    // when
    val results = Target.cleanProteinIds(input)

    // then
    results should contain inOrder ("O75949-uniprot_swissprot", "B1ALV6-uniprot", "D3DVU1-uniprot", "ENSP00000252338-ensembl_PRO")
  }
}
