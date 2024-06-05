package io.opentargets.etl.backend.facetSearch

import io.opentargets.etl.backend.target.{GeneOntologyByEnsembl, Reactomes, TractabilityWithId}
import io.opentargets.etl.backend.spark.Helpers.LocationAndSource
import io.opentargets.etl.backend.facetSearch.Helpers._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{array, col, collect_set, lit, map_values, typedLit, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}

case class TherapeuticAreasWithId(diseaseId: String, therapeuticAreas: Array[String])
object DiseaseFacets extends LazyLogging {
  def computeDiseaseNameFacets(diseaseDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing disease name facets")
    val diseaseNameFacets: Dataset[Facets] = diseaseDF
      .select(col("id"), col("name").as("label"))
      .withColumn("category", lit("Disease"))
      .withColumn("datasourceId", col("id"))
      .groupBy("label", "category", "datasourceId")
      .agg(collect_set("id").as("entityIds"))
      .distinct()
      .as[Facets]
    diseaseNameFacets
  }

  def computeTheraputicAreasFacets(diseaseDF: DataFrame)(implicit
      sparkSession: SparkSession
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
              lit("Therapeutic Area").as("category"),
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
