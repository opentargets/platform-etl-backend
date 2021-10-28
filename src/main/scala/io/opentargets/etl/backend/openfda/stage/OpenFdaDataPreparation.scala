package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.{Blacklisting, DrugData, ETLSessionContext, FdaData}
import org.apache.spark.sql.DataFrame

object OpenFdaDataPreparation extends LazyLogging {
  def apply(dfsData: IOResources)(implicit context: ETLSessionContext) = {
    implicit val sparkSession = context.sparkSession

    logger.info("OpenFDA FAERS data cooking stage - START")
    val fdaRawData = PrePrepRawFdaData(dfsData(FdaData()).data)
    // Prepare Adverse Events Data
    val fdaData = PrepareAdverseEventData(fdaRawData)
    // Prepare Drug list
    val drugList = PrepareDrugList(dfsData(DrugData()).data)
    // OpenFDA FAERS Event filtering
    val blacklistingData = PrepareBlacklistData(dfsData(Blacklisting()).data)
    val fdaFilteredData = EventsFiltering(fdaData, blacklistingData)
    // Attach drug data with linked targets information
    logger.info("Attach drug information to events")
    val fdaDataFilteredWithDrug = fdaFilteredData.join(drugList, Seq("drug_name"), "inner")
    // NOTE - CHEMBL IDs are kept 'as is', i.e. upper case, from the drug dataset through their joining with FAERS data,
    //        and they're also like that in target dataset, so no further processing is needed before joining the data.
    logger.info("OpenFDA FAERS data cooking stage - END")
    fdaDataFilteredWithDrug
  }
}
