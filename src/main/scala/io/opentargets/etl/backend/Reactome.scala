package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.graph.GraphNode
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources

// This is option/step reactome in the config file
object Reactome extends LazyLogging {

  def cleanPathways(df: DataFrame): DataFrame =
    df.filter(col("_c2") === "Homo sapiens")
      .drop("_c2")
      .toDF("id", "name")

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    val dfName = "reactome"
    val config = context.configuration.steps.reactome
    val reactomeIs = IoHelpers.readFrom(config.input)
    val pathways = reactomeIs("pathways").data.transform(cleanPathways)
    val edges = reactomeIs("relations").data.toDF("src", "dst")
    val index = GraphNode(pathways, edges).distinct

    logger.info("compute reactome dataset")
    val outputs = Map(
      dfName -> IOResource(index, config.output("reactome"))
    )

    IoHelpers.writeTo(outputs)
  }
}
