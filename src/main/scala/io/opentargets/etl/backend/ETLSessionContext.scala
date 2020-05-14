package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.OTConfig
import io.opentargets.etl.backend.SparkHelpers.getOrCreateSparkSession
import org.apache.spark.sql.SparkSession
import pureconfig.error.ConfigReaderFailures

import scala.util.Try

case class ETLSessionContext(configuration: OTConfig, sparkSession: SparkSession)

object ETLSessionContext extends LazyLogging {
  val progName: String = "ot-platform-etl"

  def apply(): Either[ConfigReaderFailures, ETLSessionContext] = {
    for {
      config <- Configuration.config
    } yield ETLSessionContext(config, getOrCreateSparkSession(progName, config.sparkUri))
  }
}
