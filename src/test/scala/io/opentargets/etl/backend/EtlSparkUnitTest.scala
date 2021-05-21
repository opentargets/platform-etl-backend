package io.opentargets.etl.backend

import io.opentargets.etl.backend.ETLSessionContext.progName
import io.opentargets.etl.backend.spark.Helpers.getOrCreateSparkSession
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers
import pureconfig.error.ConfigReaderFailures

trait EtlSparkUnitTest
    extends AnyFlatSpecLike
    with PrivateMethodTester
    with SparkSessionSetup
    with Matchers {

  private val ctxE: Either[ConfigReaderFailures, ETLSessionContext] = for {
    config <- Configuration.config
  } yield ETLSessionContext(config, sparkSession)

  val ctx = ctxE match {
    case Left(value)  => sys.exit(1)
    case Right(value) => value
  }
}
