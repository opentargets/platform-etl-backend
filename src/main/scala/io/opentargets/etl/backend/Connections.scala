package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// This is option/step eco in the config file
object Connections extends LazyLogging {
  def diseaseEdges(df: DataFrame): DataFrame = {
    df.selectExpr(
      "id",
      "parents"
    ).withColumn("type", lit("relation"))
      .withColumn("cat", lit("parent_of"))
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession

    val dfName = "edges"
    val common = context.configuration.common

    val dEdges = diseaseEdges(Disease.compute())

    val outputs = Seq(dfName)
    val outputConfs = outputs
      .map(
        name =>
          name -> IOResourceConfig(common.outputFormat, common.output + s"/$name")
      ).toMap

    val outputDFs = (outputs zip Seq(dEdges)).toMap
    SparkHelpers.writeTo(outputConfs, outputDFs)
  }
}
