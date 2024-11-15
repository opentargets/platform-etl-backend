package io.opentargets.etl.backend.so

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SequenceOntology extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession
    logger.info("Start of sequence ontology step")

    val soConfig = context.configuration.so

    val soInputs = Map(
      "nodes" -> soConfig.inputs.nodes
    )

    logger.info("Reading sequence ontology inputs")
    val inputsDfs = IoHelpers.readFrom(soInputs)

    val nodesDf = inputsDfs("nodes").data

    val idAtValue = element_at(split(col("id"), "/"), -1)
    val id = element_at(split(idAtValue, "_"), 2)
    val idPrefix = element_at(split(idAtValue, "_"), 1)

    logger.info("Extracting the id and the label from the nodes")
    val idLabelDf = nodesDf.select(
      concat(idPrefix, lit(":"), id).as("id"),
      col("lbl").as("label")
    )

    val outputs = Map(
      "nodes" -> IOResource(idLabelDf, soConfig.output)
    )

    logger.info("Writing sequence ontology outputs")
    IoHelpers.writeTo(outputs)

    logger.info("End of sequence ontology step")
  }
}
