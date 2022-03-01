package io.opentargets.etl

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpecLike
import io.opentargets.etl.backend.Configuration
import io.opentargets.etl.backend.Configuration._
import org.scalatest.matchers.must.Matchers
import pureconfig.ConfigReader

class ConfigurationTest extends AnyFlatSpecLike with Matchers with LazyLogging {
  "Pureconfig" should "successfully load standard configuration without error" in {
    val conf: ConfigReader.Result[OTConfig] = Configuration.config
    val msg = conf match {
      case Right(_) =>
        logger.info("configuration loaded right")
        None
      case Left(ex) =>
        logger.info(s"Failed to load configuration in ${ex.prettyPrint()}")
        Some(ex.prettyPrint())
    }

    assert(conf.isRight, s"Failed with ${msg.getOrElse("")}")
  }

  "SparkSettings" should "only accept valid write modes as input parameters" in {
    // given
    val invalidMode = "concatenate"
    val validModes = Seq("error", "errorifexists", "append", "overwrite", "ignore")
    // when
    val settings = validModes.map(SparkSettings(_, ignoreIfExists = false))
    // then
    assertThrows[IllegalArgumentException](SparkSettings(invalidMode, ignoreIfExists = false))
    assertResult(validModes.sorted)(settings.map(_.writeMode).sorted)
  }
}
