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
      )
      .withColumn("parent", explode(col("parents")))
      .where(col("parent").isNotNull)
      .withColumn("type", lit("relation"))
      .withColumn("cat", lit("parent_of"))
      .withColumn("_from", col("parent"))
      .withColumn("_to", col("id"))
      .drop("parents", "parent", "id")
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession

    val dfName = "disease_edges"
    val common = context.configuration.common

    val dEdges = diseaseEdges(Disease.compute())

    val outputDFs = Seq(dEdges)
    val outputs = Seq(dfName)

    val outputConfs = outputs
      .map(
        name => name -> IOResourceConfig(common.outputFormat, common.output + s"/$name")
      )
      .toMap

    val outputDFConfs = (outputs zip outputDFs).toMap
    SparkHelpers.writeTo(outputConfs, outputDFConfs)
  }
}
