package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.FdaData
import org.apache.spark.sql.DataFrame

object PrePrepRawFdaData extends LazyLogging {
  def apply(dfFda: DataFrame) = {
    logger.info("Reducing dimensions of FAERS dataset")
    // FDA Raw data pre-prep
    val columns = Seq(
      "safetyreportid",
      "serious",
      "seriousnessdeath",
      "receivedate",
      "primarysource.qualification as qualification",
      "patient"
    )
    val fdaRawData = dfFda.selectExpr(columns: _*)

    fdaRawData
  }
}
