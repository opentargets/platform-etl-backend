package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.spark.{IOResourceConfig, IoHelpers}
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

    val input = context.configuration.steps.openfda.input

    val commonData = Map(
      DrugData() -> input("chembl_drugs"),
      Blacklisting() -> input("blacklisted_events"),
      FdaData() -> input("fda_data")
    )

    val sourceData =
      if (input.contains("meddra_preferred_terms") && input.contains("meddra_low_level_terms")) {
        commonData ++ Map(
          MeddraPreferredTermsData() -> input("meddra_preferred_terms"),
          MeddraLowLevelTermsData() -> input("meddra_low_level_terms")
        )
      } else {
        commonData
      }

    // Load the data
    IoHelpers.readFrom(sourceData)
  }
}
