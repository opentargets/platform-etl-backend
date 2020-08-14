package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Association.{computeAssociationsPerDS, prepareEvidences}
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

  def associationEdges(df: DataFrame): DataFrame = {
    df.selectExpr(
      "datasource_id as cat",
      "disease_id",
      "target_id")
      .withColumn("type", lit("association"))
      .withColumn("_from",col("disease_id"))
      .withColumn("_to", col("target_id"))
      .drop("disease_id", "target_id")
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession

    val common = context.configuration.common

    val dEdges = diseaseEdges(Disease.compute())

    val assocEdges =
      associationEdges(
        computeAssociationsPerDS(
          prepareEvidences()))

    val outputDFs = Seq(dEdges)
    val outputs = Map(
      "disease_connections" -> dEdges,
      "association_connections" -> assocEdges
    )

    val outputConfs = outputs.keys
      .map(
        name => name -> IOResourceConfig(common.outputFormat, common.output + s"/$name")
      )
      .toMap

    SparkHelpers.writeTo(outputConfs, outputs)
  }
}