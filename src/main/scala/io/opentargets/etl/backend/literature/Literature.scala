package io.opentargets.etl.backend.literature

import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.Helpers.getOrCreateSparkSession
import io.opentargets.etl.backend.ETLSessionContext.progName
import com.typesafe.scalalogging.LazyLogging

object Literature extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {

    val etlSessionContext: ETLSessionContext = createETLSession()

    val input = context.configuration.steps.literature.input.filter(_._1.startsWith("embedding_"))
    logger.info(s"INPUT DATASETS: ${input.toString()}")

    // runSteps(etlSessionContext)

  }

  def createETLSession()(implicit context: ETLSessionContext): ETLSessionContext = {
    val config = context.configuration

    val configurations = config.sparkSettings.defaultSparkSessionConfig

    val litConfigurations =
      configurations ++ config.steps.literature.common.sparkSessionConfig.getOrElse(Seq())

    val etlSessionContext = ETLSessionContext(
      config,
      getOrCreateSparkSession(progName, litConfigurations, config.sparkUri)
    )
    etlSessionContext
  }

  def runSteps(etlSessionContext: ETLSessionContext): Unit = {
    implicit val context: ETLSessionContext = etlSessionContext

    logger.info("Run processing step")
    Processing()
    logger.info("Run literature embedding")
    Embedding()
    logger.info("Run literature vectors")
    Vectors()
    logger.info("Run literature epmc")
    Epmc()
  }

}
