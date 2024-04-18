package io.opentargets.etl.backend.facetSearch

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.facetSearch.TargetFacets._
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{IOResources, readFrom, writeTo}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Case class representing the facets of a target or disease.
  *
  * @param label
  *   The label of the facet. (searchable field)
  * @param category
  *   The category of the facet. (searchable field)
  * @param entityIds
  *   The entity IDs (target or disease IDs) associated with the facet.
  * @param datasourceId
  *   The datasource ID associated with the facet. (searchable field)
  */
case class Facets(
    label: String,
    category: String,
    entityIds: Seq[Option[String]],
    datasourceId: Option[String]
)

/** Object FacetSearch is used to compute facets for targets and diseases.
  */
object FacetSearch extends LazyLogging {

  /** Main function to compute facets for targets and diseases.
    *
    * @param context
    *   Implicit ETLSessionContext.
    */
  def apply()(implicit context: ETLSessionContext): Unit = {
    implicit val ss: SparkSession = context.sparkSession
    val inputs = readInputs
    val facetSearchTarget = computeFacetsTarget(inputs)
    val facetSearchDisease = computeFacetsDisease(inputs)
    writeOutput(facetSearchTarget, facetSearchDisease)
  }

  /** Read inputs from the specified sources.
    *
    * @param context
    *   Implicit ETLSessionContext.
    * @return
    *   IOResources containing the input data.
    */
  def readInputs()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    val config = context.configuration.facetSearch.inputs
    val mappedInputs = Map(
      "targets" -> config.targets,
      "diseases" -> config.diseases
    )
    readFrom(mappedInputs)
  }

  /** Compute facets for targets.
    *
    * @param inputs
    *   IOResources containing the input data.
    * @param ss
    *   Implicit SparkSession.
    * @return
    *   DataFrame containing the computed facets for targets.
    */
  private def computeFacetsTarget(inputs: IOResources)(implicit ss: SparkSession): DataFrame = {
    val targetsDF = inputs("targets").data
    val targetFacetsDatasets = Seq(
      computeTargetIdFacets(targetsDF),
      computeApprovedSymbolFacets(targetsDF),
      computeApprovedNameFacets(targetsDF),
      computeSubcellularLocationsFacets(targetsDF),
      computeTractabilityFacets(targetsDF)
    )
    val targetFacetsDF = targetFacetsDatasets.reduce(_ unionByName _).toDF()
    targetFacetsDF
  }

  /** Compute facets for diseases.
    *
    * @param inputs
    *   IOResources containing the input data.
    * @param ss
    *   Implicit SparkSession.
    * @return
    *   DataFrame containing the computed facets for diseases.
    */
  private def computeFacetsDisease(inputs: IOResources)(implicit ss: SparkSession): DataFrame = {
    val diseaseDF = inputs("diseases").data.limit(10) // TODO: remove limit
    diseaseDF
  }

  /** Write the computed facets to the specified outputs.
    *
    * @param facetSearchTarget
    *   DataFrame containing the computed facets for targets.
    * @param facetSearchDisease
    *   DataFrame containing the computed facets for diseases.
    * @param context
    *   Implicit ETLSessionContext.
    */
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
