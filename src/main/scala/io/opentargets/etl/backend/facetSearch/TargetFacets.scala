package io.opentargets.etl.backend.facetSearch

import io.opentargets.etl.backend.target.TractabilityWithId
import io.opentargets.etl.backend.spark.Helpers.LocationAndSource
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{array, col, collect_set, lit, map_values, typedLit, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

case class SubcellularLocationWithId(ensemblGeneId: String,
                                     subcellularLocations: Array[LocationAndSource]
)

case class TargetClass(id: Long, label: String, level: String)
case class TargetClassWithId(ensemblGeneId: String, targetClass: Array[TargetClass])

/** Object TargetFacets is used to compute various facets of targets.
  */
object TargetFacets extends LazyLogging {

  /** Compute tractability facets for the given targets DataFrame.
    *
    * @param targetsDF
    *   DataFrame of targets.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  def computeTractabilityFacets(
      targetsDF: DataFrame
  )(implicit sparkSession: SparkSession): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing tractability facets")
    val tractabilityModalityMappings: Column = typedLit(
      Map(
        "SM" -> "Tractability Small Molecule",
        "AB" -> "Tractability Antibody",
        "PR" -> "Tractability PROTAC",
        "OC" -> "Tractability Other Modalities"
      )
    )
    val tractabilityWithId: Dataset[TractabilityWithId] =
      targetsDF
        .select(col("id").as("ensemblGeneId"), col("tractability"))
        .where(col("tractability").isNotNull)
        .as[TractabilityWithId]
    val tractabilityFacets: Dataset[Facets] = tractabilityWithId
      .flatMap(row => row.tractability.map(t => (row.ensemblGeneId, t.modality, t.id, t.value)))
      .toDF("ensemblGeneId", "category", "label", "value")
      .where(col("value") === true)
      .groupBy("category", "label")
      .agg(collect_set("ensemblGeneId").as("entityIds"))
      .drop("value")
      .withColumn("category",
                  when(tractabilityModalityMappings($"category").isNotNull,
                       tractabilityModalityMappings($"category")
                  ).otherwise($"category")
      )
      .withColumn("datasourceId", lit(null).cast("string"))
      .as[Facets]
    tractabilityFacets
  }

  /** Compute target id facets for the given targets DataFrame.
    *
    * @param targetsDF
    *   DataFrame of targets.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  def computeTargetIdFacets(targetsDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    logger.info("Computing target id facets")
    computeSimpleFacet(targetsDF, "id", "Target ID", "id")
  }

  /** Compute approved symbol facets for the given targets DataFrame.
    *
    * @param targetsDF
    *   DataFrame of targets.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  def computeApprovedSymbolFacets(targetsDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    logger.info("Computing approved symbol facets")
    computeSimpleFacet(targetsDF, "approvedSymbol", "Approved Symbol", "id")
  }

  /** Compute approved name facets for the given targets DataFrame.
    *
    * @param targetsDF
    *   DataFrame of targets.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  def computeApprovedNameFacets(targetsDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    logger.info("Computing approved name facets")
    computeSimpleFacet(targetsDF, "approvedName", "Approved Name", "id")
  }

  def computeSubcellularLocationsFacets(targetsDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing subcellular locations facets")
    val subcellularLocationWithId: Dataset[SubcellularLocationWithId] =
      targetsDF
        .select(col("id").as("ensemblGeneId"), col("subcellularLocations"))
        .where(col("subcellularLocations").isNotNull)
        .as[SubcellularLocationWithId]
    val subcellularLocationsFacets: Dataset[Facets] = subcellularLocationWithId
      .flatMap(row =>
        row.subcellularLocations.map(s =>
          (row.ensemblGeneId, s.location, "Subcellular Location", s.termSl)
        )
      )
      .toDF("ensemblGeneId", "label", "category", "datasourceId")
      .groupBy("label", "category", "datasourceId")
      .agg(collect_set("ensemblGeneId").as("entityIds"))
      .as[Facets]
    subcellularLocationsFacets
  }

  def computeTargetClassFacets(targetsDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing target class facets")
    val targetClassWithId: Dataset[TargetClassWithId] =
      targetsDF
        .select(col("id").as("ensemblGeneId"), col("targetClass"))
        .where(col("targetClass").isNotNull)
        .as[TargetClassWithId]
    val targetClassFacets: Dataset[Facets] = targetClassWithId
      .flatMap(row => row.targetClass.map(t => (row.ensemblGeneId, t.label, "ChEMBL Target Class")))
      .toDF("ensemblGeneId", "label", "category")
      .groupBy("label", "category")
      .agg(collect_set("ensemblGeneId").as("entityIds"))
      .withColumn("datasourceId", lit(null).cast("string"))
      .as[Facets]
    targetClassFacets
  }

  /** Compute simple facet dataset for the given DataFrame, setting the datasourceId to null.
    *
    * @param dataframe
    *   DataFrame to compute facets from.
    * @param labelField
    *   Field to use as label.
    * @param categoryField
    *   Value to use as category.
    * @param entityIdField
    *   Field to use as entity id.
    * @param sparkSession
    *   Implicit SparkSession.
    * @return
    *   Dataset of Facets.
    */
  private def computeSimpleFacet(dataframe: DataFrame,
                                 labelField: String,
                                 categoryField: String,
                                 entityIdField: String
  )(implicit sparkSession: SparkSession): Dataset[Facets] = {
    import sparkSession.implicits._

    val facets: Dataset[Facets] = dataframe
      .select(
        col(labelField).as("label"),
        lit(categoryField).as("category"),
        col(entityIdField).as("id")
      )
      .groupBy("label", "category")
      .agg(collect_set("id").as("entityIds"))
      .withColumn("datasourceId", lit(null).cast("string"))
      .as[Facets]
    facets
  }

}
