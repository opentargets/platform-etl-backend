package io.opentargets.etl.backend.searchFacet

import io.opentargets.etl.backend.searchFacet.Helpers._
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.Configuration.FacetSearchCategories
import org.apache.spark.sql.functions.{col, collect_set, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TherapeuticAreasWithId(diseaseId: String, therapeuticAreas: Array[String])
object DiseaseFacets extends LazyLogging {

  /** Compute disease facets for the given disease DataFrame.
    *
    * @param diseaseDF
    *   DataFrame of diseases.
    * @param categoryValues
    *   FacetSearchCategories.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  def computeDiseaseNameFacets(diseaseDF: DataFrame, categoryValues: FacetSearchCategories)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing disease name facets")
    val diseaseNameFacets: Dataset[Facets] = diseaseDF
      .select(col("id"), col("name").as("label"))
      .withColumn("category", lit(categoryValues.diseaseName))
      .withColumn("datasourceId", col("id"))
      .groupBy("label", "category", "datasourceId")
      .agg(collect_set("id").as("entityIds"))
      .distinct()
      .as[Facets]
    diseaseNameFacets
  }

  /** Compute therapeutic areas facets for the given disease DataFrame.
    *
    * @param diseaseDF
    *   DataFrame of diseases.
    * @param categoryValues
    *   FacetSearchCategories.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  def computeTherapeuticAreasFacets(diseaseDF: DataFrame, categoryValues: FacetSearchCategories)(
      implicit sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing therapeutic areas facets")
    val diseaseNames: DataFrame = diseaseDF
      .select(col("id"), col("name"))
    val therapeuticAreasWithId: Dataset[TherapeuticAreasWithId] =
      getRelevantDataset[TherapeuticAreasWithId](diseaseDF, "id", "diseaseId", "therapeuticAreas")
    val therapeuticAreasFacets: Dataset[Facets] = therapeuticAreasWithId
      .flatMap(row => row.therapeuticAreas.map(t => (row.diseaseId, t)))
      .toDF("diseaseId", "tId")
      .join(diseaseNames, col("tId") === col("id"))
      .select(col("name").as("label"),
              lit(categoryValues.therapeuticArea).as("category"),
              col("diseaseId"),
              col("tId").as("datasourceId")
      )
      .groupBy("label", "category", "datasourceId")
      .agg(collect_set("diseaseId").as("entityIds"))
      .distinct()
      .as[Facets]
    therapeuticAreasFacets
  }

}
