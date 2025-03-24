package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.OTConfig
import io.opentargets.etl.backend.spark.Helpers.getOrCreateSparkSession
import org.apache.spark.sql.SparkSession
import pureconfig.error.ConfigReaderFailures

case class ETLSessionContext(configuration: OTConfig, sparkSession: SparkSession)

object ETLSessionContext extends LazyLogging {
  val progName: String = "ot-platform-etl"

  def apply(): Either[ConfigReaderFailures, ETLSessionContext] =
    for {
      config <- Configuration.config
    } yield {
      logger.info("Generating ETL Session Context")
      val evidence = if (config.evidence.dataSourcesExclude.nonEmpty) {
        logger.info(s"Excluding data sources for evidence: ${config.evidence.dataSourcesExclude}")
        val ds = config.evidence.dataSources.filter(d =>
          !config.evidence.dataSourcesExclude.contains(d.id)
        )
        config.evidence.copy(dataSources = ds)
      } else config.evidence

      val configurations = config.sparkSettings.defaultSparkSessionConfig

      ETLSessionContext(
        config.copy(evidence = evidence),
        getOrCreateSparkSession(progName, configurations, config.sparkUri)
      )
    }
}
