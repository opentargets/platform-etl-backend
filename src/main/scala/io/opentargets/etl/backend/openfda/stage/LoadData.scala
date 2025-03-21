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

    val input = context.configuration.openfda.input

    val commonData = Map(
      DrugData() -> input("chembl-drugs"),
      Blacklisting() -> input("blacklisted-events"),
      FdaData() -> input("fda-data")
    )

    val sourceData =
      if (input.contains("meddra-preferred-terms") && input.contains("meddra-low-level-terms")) {
        commonData ++ Map(
          MeddraPreferredTermsData() -> input("meddra-preferred-terms"),
          MeddraLowLevelTermsData() -> input("meddra-low-level-terms")
        )
      } else {
        commonData
      }

    // Load the data
    IoHelpers.readFrom(sourceData)
  }
}
