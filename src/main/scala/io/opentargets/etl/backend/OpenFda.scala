package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.OpenFdaEtl
import io.opentargets.etl.backend.openfda.stage.MonteCarloSampling
import io.opentargets.etl.backend.openfda.utils.Writers
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/*
    Project     : io-opentargets-etl-backend
    Timestamp   : 2021-07-27T11:37
    Author      : Manuel Bernal Llinares <mbdebian@gmail.com>
*/

// Data Sources
sealed trait DataSource
case object DrugData extends DataSource {
  def apply(): String = "drugData"
}
case object Blacklisting extends DataSource {
  def apply(): String = "blacklisting"
}
case object FdaData extends DataSource {
  def apply(): String = "fdaData"
}
case object MeddraData extends DataSource {
  def apply(): String = "meddraData"
}

object OpenFda extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val sparkSession = context.sparkSession
    // TODO - REFACTORING -
    // TODO --- Load the data ---
    // TODO - Drug data
    // TODO - Blacklisting
    // TODO - OpenFDA FAERS source data
    // TODO --- Data transformation ---
    // TODO - Drug list
    // TODO - OpenFDA FAERS
    // TODO - Adverse Events

    val fdaConfig = context.configuration.openfda
    logger.info("Aggregating FDA data...")
    val openFdaDataAggByChembl: DataFrame =
      OpenFdaEtl(context)

    logger.info("Performing Monte Carlo sampling...")
    // TODO - Refactor this into the ETL itself
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
