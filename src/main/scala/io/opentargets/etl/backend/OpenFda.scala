package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.OpenFdaEtl
import io.opentargets.etl.backend.openfda.stage.{EventsFiltering, LoadData, MonteCarloSampling, PrepareAdverseEventData, PrepareDrugList, PrepareSummaryStatistics}
import io.opentargets.etl.backend.openfda.utils.Writers
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/*
    Project     : io-opentargets-etl-backend
    Timestamp   : 2021-07-27T11:37
    Author      : Manuel Bernal Llinares <mbdebian@gmail.com>
*/

// Data Sources
sealed trait FdaDataSource
case object DrugData extends FdaDataSource {
  def apply(): String = "drugData"
}
case object Blacklisting extends FdaDataSource {
  def apply(): String = "blacklisting"
}
case object FdaData extends FdaDataSource {
  def apply(): String = "fdaData"
}
case object MeddraData extends FdaDataSource {
  def apply(): String = "meddraData"
}

// OpenFDA FAERS ETL Step
object OpenFda extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val sparkSession = context.sparkSession
    // TODO - REFACTORING -
    // --- Load the data ---
    val dfsData = LoadData()
    // TODO --- Data transformation ---
    // Prepare Adverse Events Data
    val fdaData = PrepareAdverseEventData(dfsData(FdaData()).data)
    // Prepare Drug list
    val drugList = PrepareDrugList(dfsData(DrugData()).data)
    // OpenFDA FAERS Event filtering
    val fdaFilteredData = EventsFiltering(fdaData, dfsData(Blacklisting()).data)
    // Attach drug data
    val fdaDataFilteredWithDrug = fdaFilteredData.join(drugList, Seq("drug_name"), "inner")
    // Prepare Summary Statistics
    val fdaDataWithSummaryStats = PrepareSummaryStatistics(fdaDataFilteredWithDrug)
    // TODO - Montecarlo data preparation
    // TODO - Add Meddra
    // TODO - Compute Montecarlo Sampling
    // TODO - Produce Output

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
