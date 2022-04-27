package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.OTConfig
import io.opentargets.etl.backend.spark.Helpers.getOrCreateSparkSession
import org.apache.spark.sql.SparkSession
import pureconfig.error.ConfigReaderFailures

case class ETLSessionContext(configuration: OTConfig, sparkSession: SparkSession)

object ETLSessionContext extends LazyLogging {
  val progName: String = "ot-platform-etl"

  def apply(): Either[ConfigReaderFailures, ETLSessionContext] = {
    for {
      config <- Configuration.config
    } yield {
      logger.info("Generating ETL Session Context")
      val evidence = if (config.evidences.dataSourcesExclude.nonEmpty) {
        logger.info(s"Excluding data sources for evidence: ${config.evidences.dataSourcesExclude}")
        val ds = config.evidences.dataSources.filter(d =>
          !config.evidences.dataSourcesExclude.contains(d.id)
        )
        config.evidences.copy(dataSources = ds)
      } else config.evidences
      ETLSessionContext(
        config.copy(evidences = evidence),
        getOrCreateSparkSession(progName, config.sparkUri)
      )
    }
  }
}
