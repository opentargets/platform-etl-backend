package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.stage.{AttachMeddraData, EventsFiltering, LoadData, MonteCarloSampling, OpenFdaDataPreparation, OpenFdaDrugs, OpenFdaTargets, PrePrepRawFdaData, PrepareAdverseEventData, PrepareBlacklistData, PrepareDrugList, PrepareForMontecarlo, PrepareSummaryStatistics, StratifiedSampling}
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
    // Data loading stage
    logger.info("OpenFDA FAERS data loading")
    val dfsData = LoadData()

    // Data Preparation (cooking)
    val fdaCookedData = OpenFdaDataPreparation(dfsData)

    // --- Run OpenFDA FAERS for drugs ---
    OpenFdaDrugs(dfsData, fdaCookedData)
    // Run OpenFDA FAERS for targets
    OpenFdaTargets(dfsData, fdaCookedData)
    logger.info("OpenFDA FAERS step completed")
  }
}
