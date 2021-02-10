package io.opentargets.etl.backend.target

import better.files.File
import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig}
import io.opentargets.etl.backend.target.UniprotTest.uniprotDataPath

object TargetTest {}

class TargetTest extends EtlSparkUnitTest {
  "Target" should "read in raw Uniprot data and preprocessing complete" in {
    // given
    val ioConf = IOResourceConfig("txt", uniprotDataPath)
    val getUniprotDataFrame = PrivateMethod[IOResource]('getUniprotDataFrame)
    // when
    val results = io.opentargets.etl.backend.target.Target invokePrivate getUniprotDataFrame(
      ioConf,
      sparkSession)
    // then
    results.data.count() must be(10)
  }
}

object UniprotTest {
  val uniprotDataPath: String = this.getClass.getResource("/uniprot/sample_10.txt").getPath
  lazy val uniprotDataStream: Iterator[String] = File(uniprotDataPath).lineIterator
}

class UniprotTest extends EtlSparkUnitTest {}
