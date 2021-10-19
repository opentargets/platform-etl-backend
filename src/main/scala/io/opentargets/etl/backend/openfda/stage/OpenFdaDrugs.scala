package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.DataFrame

object OpenFdaDrugs {

  def apply(dfsData: IOResources, fdaDataFilteredWithDrug: DataFrame)(implicit context: ETLSessionContext) = ???
  
}
