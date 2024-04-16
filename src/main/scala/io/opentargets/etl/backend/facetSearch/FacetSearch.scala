package io.opentargets.etl.backend.facetSearch

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{IOResources, readFrom, writeTo}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object FacetSearch extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession

    val inputs = readInputs

    val facetSearchTarget = computeFacetsTarget(inputs)
    val facetSearchDisease = computeFacetsDisease(inputs)

    writeOutput(facetSearchTarget, facetSearchDisease)
  }

  def readInputs()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val config = context.configuration.facetSearch.inputs

    val mappedInputs = Map(
      "targets" -> config.targets,
      "diseases" -> config.diseases
    )

    readFrom(mappedInputs)

  }

  def computeFacetsTarget(inputs: IOResources)(implicit ss: SparkSession): DataFrame = {
    val targetsDF = inputs("targets").data
    targetsDF
  }

  def computeFacetsDisease(inputs: IOResources)(implicit ss: SparkSession): DataFrame = {
    val diseaseDF = inputs("diseases").data
    diseaseDF
  }
  def writeOutput(facetSearchTarget: DataFrame, facetSearchDisease: DataFrame)(implicit
      context: ETLSessionContext
  ): Unit = {
    val outputConfig = context.configuration.facetSearch.outputs

    val outputs = Map(
      "facetSearchTarget" -> IOResource(facetSearchTarget, outputConfig.targets),
      "facetSearchDisease" -> IOResource(facetSearchDisease, outputConfig.diseases)
    )

    writeTo(outputs)
  }

}
