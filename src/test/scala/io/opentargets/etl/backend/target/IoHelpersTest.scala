package io.opentargets.etl.backend.target
import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class IoHelpersTest extends EtlSparkUnitTest {

  "Additional formats" should "be added to IOResources outputs" in {
    // given
    import sparkSession.implicits._
    val addFormats = PrivateMethod[IOResources]('addAdditionalOutputFormats)
    val df = Seq(1).toDF
    val resources: IOResources = Map("one" -> IOResource(df, IOResourceConfig("f1", "p1/f1/out")))
    val configs: List[String] = List("f2")

    // when
    val result = IoHelpers invokePrivate addFormats(resources, configs, "parquet")

    // then
    // an entry created for each format
    result should have size (2)
    // the dataframe is cached
    result.head._2.data.storageLevel.useMemory should be(true)
  }
}
