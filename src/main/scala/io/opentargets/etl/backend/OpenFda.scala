package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.stage.{AttachMeddraData, EventsFiltering, LoadData, MonteCarloSampling, PrePrepRawFdaData, PrepareAdverseEventData, PrepareBlacklistData, PrepareDrugList, PrepareForMontecarlo, PrepareSummaryStatistics, StratifiedSampling}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.storage.StorageLevel

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

    // Load the data
    val dfsData = LoadData()
    val fdaRawData = PrePrepRawFdaData(dfsData(FdaData()).data)
    // Prepare Adverse Events Data
    val fdaData = PrepareAdverseEventData(fdaRawData)
    // Prepare Drug list
    val drugList = PrepareDrugList(dfsData(DrugData()).data)
    // OpenFDA FAERS Event filtering
    val blacklistingData = PrepareBlacklistData(dfsData(Blacklisting()).data)
    val fdaFilteredData = EventsFiltering(fdaData, blacklistingData)
    // Attach drug data
    val fdaDataFilteredWithDrug = fdaFilteredData.join(drugList, Seq("drug_name"), "inner")
    // Prepare Summary Statistics
    val fdaDataWithSummaryStats = PrepareSummaryStatistics(fdaDataFilteredWithDrug)
    // Montecarlo data preparation
    val fdaDataMontecarloReady = PrepareForMontecarlo(fdaDataWithSummaryStats)
    // Add Meddra
    val fdaDataWithMeddra = context.configuration.openfda.meddra match {
      case Some(_) => AttachMeddraData(fdaDataMontecarloReady,
        dfsData(MeddraPreferredTermsData()).data,
        dfsData(MeddraLowLevelTermsData()).data).persist(StorageLevel.MEMORY_AND_DISK_SER)
      case _ => fdaDataMontecarloReady
        .withColumn("meddraCode", typedLit[String](""))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    // Conditional generation of Stratified Sampling
    if (context.configuration.openfda.sampling.enabled) {
      // This one really uses the raw OpenFDA Data
      StratifiedSampling(dfsData(FdaData()).data, fdaDataWithSummaryStats, fdaDataWithMeddra)
    }
    // Compute Montecarlo Sampling
    val montecarloResults = MonteCarloSampling(
      fdaDataWithMeddra,
      context.configuration.openfda.montecarlo.percentile,
      context.configuration.openfda.montecarlo.permutations
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // Produce Output
    val outputMap: IOResources = Map(
      "unfiltered" -> IOResource(fdaDataWithMeddra, context.configuration.openfda.outputs.fdaUnfiltered),
      "openFdaResults" -> IOResource(montecarloResults, context.configuration.openfda.outputs.fdaResults)
    )
    IoHelpers.writeTo(outputMap)
    logger.info("OpenFDA FAERS step completed")
  }
}
