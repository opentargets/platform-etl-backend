package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.spark.IoHelpers
import io.opentargets.etl.backend.{
  Blacklisting,
  DrugData,
  ETLSessionContext,
  FdaData,
  MeddraLowLevelTermsData,
  MeddraPreferredTermsData
}

object LoadData {
  def apply()(implicit context: ETLSessionContext) = {

    // Get the Spark Session
    implicit val sparkSession = context.sparkSession

    // Prepare the loading Map
    val sourceData =
      context.configuration.openfda.meddra match {
        // DISCLAIMER - There's probably a better way to do this
        case Some(meddraConfig) =>
          Map(
            DrugData() -> context.configuration.openfda.chemblDrugs,
            Blacklisting() -> context.configuration.openfda.blacklistedEvents,
            FdaData() -> context.configuration.openfda.fdaData,
            MeddraPreferredTermsData() -> meddraConfig.meddraPreferredTerms,
            MeddraLowLevelTermsData() -> meddraConfig.meddraLowLevelTerms
          )
        case _ =>
          Map(
            DrugData() -> context.configuration.openfda.chemblDrugs,
            Blacklisting() -> context.configuration.openfda.blacklistedEvents,
            FdaData() -> context.configuration.openfda.fdaData
          )
      }
    // Load the data
    IoHelpers.readFrom(sourceData)
  }
}
