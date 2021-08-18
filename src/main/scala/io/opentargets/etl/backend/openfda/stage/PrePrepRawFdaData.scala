package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.FdaData
import org.apache.spark.sql.DataFrame

object PrePrepRawFdaData {
  def apply(dfFda: DataFrame) = {
    // FDA Raw data pre-prep
    val columns = Seq("safetyreportid",
      "serious",
      "seriousnessdeath",
      "receivedate",
      "primarysource.qualification as qualification",
      "patient")
    val fdaRawData = dfFda.selectExpr(columns: _*)

    fdaRawData
  }
}
