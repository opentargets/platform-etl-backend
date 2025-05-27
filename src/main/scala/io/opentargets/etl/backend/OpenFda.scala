package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.openfda.stage.{LoadData, OpenFdaCompute, OpenFdaDataPreparation}
import io.opentargets.etl.backend.spark.IOResourceConfig
import org.apache.spark.sql.functions.explode

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

// Target Map Structure
case class TargetDimension(
    colId: String,
    statsColId: String,
    outputUnfilteredResults: IOResourceConfig,
    outputResults: IOResourceConfig
)

// OpenFDA FAERS ETL Step
object OpenFda extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val sparkSession = context.sparkSession
    import sparkSession.implicits._
    val config = context.configuration.steps.openfda

    // --- Massage OpenFDA FAERS and drug data ---
    // Data loading stage
    logger.info("OpenFDA FAERS data loading")
    val dfsData = LoadData()

    // Data Preparation (cooking)
    val fdaCookedData = OpenFdaDataPreparation(dfsData)
    // fdaCookedData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // --- Run OpenFDA FAERS for drugs ---
    OpenFdaCompute(
      dfsData,
      fdaCookedData,
      TargetDimension(
        "chembl_id",
        "uniq_report_ids_by_drug",
        config.output("fda-unfiltered"),
        config.output("fda-results")
      )
    )
    // --- Run OpenFDA FAERS for targets ---
    // We'll use only those reports with associated target information
    val fdaDataTargets = fdaCookedData
      .where($"linkedTargets".isNotNull)
      .withColumn("targetId", explode($"linkedTargets.rows"))
      .drop($"linkedTargets")
    OpenFdaCompute(
      dfsData,
      fdaDataTargets,
      TargetDimension(
        "targetId",
        "uniq_report_ids_by_target",
        config.output("fda-targets-unfiltered"),
        config.output("fda-targets-results")
      )
    )
    logger.info("OpenFDA FAERS step completed")
  }
}
