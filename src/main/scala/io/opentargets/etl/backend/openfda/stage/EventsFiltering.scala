package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast

/*
    Project     : io-opentargets-etl-backend
    Timestamp   : 2021-08-04T11:58
    Author      : Manuel Bernal Llinares <mbdebian@gmail.com>
*/

object EventsFiltering {
  def apply(dfFdaEvents: DataFrame, dfBlackList: DataFrame)(implicit context: ETLSessionContext) = {
    dfFdaEvents
      .join(dfBlackList, dfFdaEvents("reaction_reactionmeddrapt") === dfBlackList("reactions"), "left_anti")
  }
}
