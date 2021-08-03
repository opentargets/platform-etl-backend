package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.stage.{MonteCarloSampling, OpenFdaEtl}
import io.opentargets.etl.backend.openfda.utils.Writers
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/*
    Project     : io-opentargets-etl-backend
    Timestamp   : 2021-07-27T11:37
    Author      : Manuel Bernal Llinares <mbdebian@gmail.com>
*/

object OpenFda extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val sparkSession = context.sparkSession
    // TODO - REFACTORING -
    val fdaConfig = context.configuration.openfda
    logger.info("Aggregating FDA data...")
    val openFdaDataAggByChembl: DataFrame =
      OpenFdaEtl(context)

    logger.info("Performing Monte Carlo sampling...")
    val mcResults =
      MonteCarloSampling(
        openFdaDataAggByChembl,
        fdaConfig.montecarlo.percentile,
        fdaConfig.montecarlo.permutations).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Writing results of FDA pipeline
    if (fdaConfig.outputFormats.nonEmpty) {
      fdaConfig.outputFormats.foreach { extension =>
        Writers.writeFdaResults(openFdaDataAggByChembl, fdaConfig.output, extension)
      }
    }
    if (fdaConfig.outputFormats.nonEmpty) {
      fdaConfig.outputFormats.foreach { extension =>
        Writers.writeMonteCarloResults(mcResults,
          fdaConfig.output,
          extension)
      }
    }

    logger.info("OpenFDA FAERS step completed")
  }
}
