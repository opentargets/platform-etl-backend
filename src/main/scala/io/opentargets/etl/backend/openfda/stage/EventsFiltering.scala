package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, lower, translate, trim}

object EventsFiltering {
  def apply(dfFdaEvents: DataFrame, dfBlackList: DataFrame) = {
    // Do the filtering
    dfFdaEvents
      .join(dfBlackList, dfFdaEvents("reaction_reactionmeddrapt") === dfBlackList("reactions"), "left_anti")
  }
}
