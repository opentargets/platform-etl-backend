package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.OpenFdaEtl
import io.opentargets.etl.backend.openfda.stage.{AttachMeddraData, EventsFiltering, LoadData, MonteCarloSampling, PrepareAdverseEventData, PrepareDrugList, PrepareForMontecarlo, PrepareSummaryStatistics}
import io.opentargets.etl.backend.openfda.utils.Writers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.typedLit
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
case object MeddraPreferredTermsData extends FdaDataSource {
  def apply(): String = "meddraPreferredTermsData"
}
case object MeddraLowLevelTermsData extends FdaDataSource {
  def apply(): String = "meddraLowLevelTermsData"
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
    // Montecarlo data preparation
    val fdaDataMontecarloReady = PrepareForMontecarlo(fdaDataWithSummaryStats)
    // Add Meddra
    val fdaDataWithMeddra = context.configuration.openfda.meddra match {
      case Some(value) => AttachMeddraData(fdaDataMontecarloReady,
        dfsData(MeddraPreferredTermsData()).data,
        dfsData(MeddraLowLevelTermsData()).data).persist(StorageLevel.MEMORY_AND_DISK_SER)
      case _ => fdaDataMontecarloReady
        .withColumn("meddraCode", typedLit[String](""))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    // TODO - Conditional generation of Stratified Sampling
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
