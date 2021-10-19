package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.stage.{AttachMeddraData, EventsFiltering, LoadData, MonteCarloSampling, OpenFdaDrugs, OpenFdaTargets, PrePrepRawFdaData, PrepareAdverseEventData, PrepareBlacklistData, PrepareDrugList, PrepareForMontecarlo, PrepareSummaryStatistics, StratifiedSampling}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.storage.StorageLevel

// Data Sources
sealed trait FdaDataSource
case object DrugData extends FdaDataSource {
  def apply(): String = "drugData"
}
case object TargetData extends FdaDataSource {
  def apply(): String = "targetData"
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

    // --- Massage OpenFDA FAERS and drug data ---
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
    // Attach drug data with linked targets information
    val fdaDataFilteredWithDrug = fdaFilteredData.join(drugList, Seq("drug_name"), "inner")
    // NOTE - CHEMBL IDs are kept 'as is', i.e. upper case, from the drug dataset through their joining with FAERS data,
    //        and they're also like that in target dataset, so no further processing is needed before joining the data.
    // --- END of Massage OpenFDA FAERS and drug data ---

    // Run OpenFDA FAERS for targets
    OpenFdaTargets(dfsData, fdaDataFilteredWithDrug)
    // --- Run OpenFDA FAERS for drugs ---
    OpenFdaDrugs(dfsData, fdaDataFilteredWithDrug)
    logger.info("OpenFDA FAERS step completed")
  }
}
