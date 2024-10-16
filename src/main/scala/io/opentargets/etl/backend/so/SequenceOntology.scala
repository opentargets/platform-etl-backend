package io.opentargets.etl.backend.so

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SequenceOntology extends LazyLogging {

  // Define schema for the array of structs
//  val keyValueSchema: ArrayType = ArrayType(StructType(List(
//    StructField("pred", StringType, nullable = true),
//    StructField("val", StringType, nullable = true)
//  )))
  private val owlPrefix = "owl:"

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss = context.sparkSession
    logger.info("Start of sequence ontology step")

    val soConfig = context.configuration.so

    val soInputs = Map(
      "nodes" -> soConfig.inputs.nodes,
      "properties" -> soConfig.inputs.properties,
      "subclasses" -> soConfig.inputs.subclasses
    )

    logger.info("Reading sequence ontology inputs")
    val inputsDfs = IoHelpers.readFrom(soInputs)

    val nodesDf = inputsDfs("nodes").data

    // Extract the values from url format for the id fields
    val idAtPrefix = element_at(split(col("id"), "/"), -2)
    val idAtValue = element_at(split(col("id"), "/"), -1)
    val id = element_at(split(idAtValue, "_"), 2)
    val idPrefix = element_at(split(idAtValue, "_"), 1)


    logger.info("Flattening nodes")
    val flattenNodesDf = nodesDf.select(
      concat(idAtPrefix, lit(":"), idAtValue).as("@id"),
      concat(idPrefix, lit(":"), id).as("id"),
      col("lbl").as("label"),
      concat(lit(owlPrefix), col("type")).as("@type"),
      col("meta.*")
    )

    logger.info("Extracting basicPropertyValues")
    val valCol = element_at(split(col("col.val"), "/"), -1).as("val")
    val predCol = element_at(split(col("col.pred"), "/"), -1).as("pred")
    val cleanPredCol = when(col("pred").contains("#"), element_at(split(col("pred"), "#"), -1))
      .otherwise(col("pred"))
    val propertiesLUT = flattenNodesDf
      .select(col("id"), explode(col("basicPropertyValues")))
      .select(col("id"), predCol, valCol)
      .withColumn("pred", cleanPredCol)
      .groupBy("id")
      .pivot("pred")
      .agg(first("val"))
    val nodesWithPropertiesDF = flattenNodesDf
      .join(propertiesLUT, Seq("id"))
      .drop("basicPropertyValues")

    logger.info("Extracting synonyms")
    val synonymsLUT = flattenNodesDf
      .select(col("id"), explode(col("Synonyms")))
      .select(col("id"), predCol, valCol)
      .withColumn("pred", cleanPredCol)
      .groupBy("id")
      .pivot("pred")
      .agg(collect_list("val"))
    val nodesWithSynonymsDF = nodesWithPropertiesDF
      .join(synonymsLUT, Seq("id"))
      .drop("Synonyms")

    logger.info("Renaming definition column")
    val propertiesDF = inputsDfs("properties").data
    val commentsColName = propertiesDF
      .where(col("lbl") === "definition")
      .select(element_at(split(col("id"), "/"), -1))
      .first()
      .getString(0)
    val nodesWithDefinitionRenamedDf = nodesWithSynonymsDF
      .select(col("*"), col("definition.val").as(commentsColName))
      .drop("definition")

    logger.info("Renaming deprecated column")
    val nodesRenamedFiledsDf = nodesWithDefinitionRenamedDf.select(col("*"),col("deprecated").as(owlPrefix+"deprecated")).drop("deprecated")

    val outputs = Map(
      "nodes" -> IOResource(nodesRenamedFiledsDf, soConfig.output)
    )

    logger.info("Writing sequence ontology outputs")
    IoHelpers.writeTo(outputs)

    logger.info("End of sequence ontology step")
  }

}
