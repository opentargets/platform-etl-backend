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
}
