package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast

object EventsFiltering {
  def apply(dfFdaEvents: DataFrame, dfBlackList: DataFrame) = {
    dfFdaEvents
      .join(dfBlackList, dfFdaEvents("reaction_reactionmeddrapt") === dfBlackList("reactions"), "left_anti")
  }
}
