package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, lower, translate, trim}

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
