package io.opentargets.etl.backend.searchFacet

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.searchFacet.DiseaseFacets._
import io.opentargets.etl.backend.searchFacet.TargetFacets._
import io.opentargets.etl.backend.spark.IOResource
import io.opentargets.etl.backend.spark.IoHelpers.{IOResources, readFrom, writeTo}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    val inputs = context.configuration.steps.searchFacet.input
    readFrom(inputs)
  }

  /** Compute facets for targets.
    *
    * @param inputs
    *   IOResources containing the input data.
    * @param context
    *   Implicit ETLSessionContext.
    * @return
    *   DataFrame containing the computed facets for targets.
    */
  private def computeFacetsTarget(
      inputs: IOResources
  )(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession
    val categoryValues = context.configuration.steps.searchFacet.categories
    val targetsDF: DataFrame = inputs("targets").data
    val goDF: DataFrame = inputs("go").data
    val targetFacetsDatasets: Seq[Dataset[Facets]] = Seq(
      computeTargetIdFacets(targetsDF, categoryValues),
      computeApprovedSymbolFacets(targetsDF, categoryValues),
      computeApprovedNameFacets(targetsDF, categoryValues),
      computeGOFacets(targetsDF, goDF, categoryValues),
      computeSubcellularLocationsFacets(targetsDF, categoryValues),
      computeTargetClassFacets(targetsDF, categoryValues),
      computePathwaysFacets(targetsDF, categoryValues),
      computeTractabilityFacets(targetsDF, categoryValues)
    )
    val targetFacetsDF: DataFrame = targetFacetsDatasets.reduce(_ unionByName _).toDF()
    targetFacetsDF.coalesce(200)
  }

  /** Compute facets for diseases.
    *
    * @param inputs
    *   IOResources containing the input data.
    * @param context
    *   Implicit ETLSessionContext.
    * @return
    *   DataFrame containing the computed facets for diseases.
    */
  private def computeFacetsDisease(
      inputs: IOResources
  )(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession
    val categoryValues = context.configuration.steps.searchFacet.categories
    val diseaseDF = inputs("diseases").data
    val diseaseFacetDatasets =
      Seq(computeDiseaseNameFacets(diseaseDF, categoryValues),
          computeTherapeuticAreasFacets(diseaseDF, categoryValues)
      )
    val diseaseFacetsDF = diseaseFacetDatasets.reduce(_ unionByName _).toDF()
    diseaseFacetsDF.coalesce(200)
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
    val outputConfig = context.configuration.steps.searchFacet.output
    val outputs = Map(
      "facetSearchTarget" -> IOResource(facetSearchTarget, outputConfig("targets")),
      "facetSearchDisease" -> IOResource(facetSearchDisease, outputConfig("diseases"))
    )
    writeTo(outputs)
  }

}
