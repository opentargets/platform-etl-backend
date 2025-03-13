package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

object EventsFiltering extends LazyLogging {
  def apply(dfFdaEvents: DataFrame, dfBlackList: DataFrame) = {
    logger.info("Filter out those events that are blacklisted")
    // Do the filtering
    dfFdaEvents
      .join(
        dfBlackList,
        dfFdaEvents("reaction_reactionmeddrapt") === dfBlackList("reactions"),
        "left_anti"
      )
  }
}
