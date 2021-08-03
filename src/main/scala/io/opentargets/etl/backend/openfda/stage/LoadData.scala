package io.opentargets.etl.backend.openfda.stage

import akka.actor.TypedActor.context
import io.opentargets.etl.backend.spark.IoHelpers
import io.opentargets.etl.backend.{Blacklisting, DrugData, ETLSessionContext, FdaData, MeddraData}
import org.apache.spark.sql.SparkSession

/*
    Project     : io-opentargets-etl-backend
    Timestamp   : 2021-08-04T00:47
    Author      : Manuel Bernal Llinares <mbdebian@gmail.com>
*/

object LoadData {
  def apply()(implicit context: ETLSessionContext) = {

    // Get the Spark Session
    implicit val sparkSession = context.sparkSession

    // Prepare the loading Map
    val sourceData = Map(
      DrugData() -> context.configuration.openfda.chemblDrugs,
      Blacklisting() -> context.configuration.openfda.blacklistedEvents,
      FdaData() -> context.configuration.openfda.fdaData,
      MeddraData() -> context.configuration.openfda.meddra
    )
    // TODO - Load the data
    IoHelpers.readFrom(sourceData)
  }
}
