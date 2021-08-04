package io.opentargets.etl.backend.openfda.stage

import akka.actor.TypedActor.context
import io.opentargets.etl.backend.spark.Helpers.IOResourceConfig
import io.opentargets.etl.backend.spark.IoHelpers
import io.opentargets.etl.backend.spark.IoHelpers.IOResourceConfigurations
import io.opentargets.etl.backend.{Blacklisting, DrugData, ETLSessionContext, FdaData, MeddraData}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Stream.Empty

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
    val sourceData = {
      context.configuration.openfda.meddra match {
          // DISCLAIMER - There's probably a better way to do this
        case Some(value) => Map(
          DrugData() -> context.configuration.openfda.chemblDrugs,
          Blacklisting() -> context.configuration.openfda.blacklistedEvents,
          FdaData() -> context.configuration.openfda.fdaData,
          MeddraData() -> value
        )
        case _ => Map(
          DrugData() -> context.configuration.openfda.chemblDrugs,
          Blacklisting() -> context.configuration.openfda.blacklistedEvents,
          FdaData() -> context.configuration.openfda.fdaData,
        )
      }

    }
    // TODO - Load the data
    IoHelpers.readFrom(sourceData)
  }
}
