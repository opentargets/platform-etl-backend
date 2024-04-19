package io.opentargets.etl.backend.facetSearch

import io.opentargets.etl.backend.target.{GeneOntologyByEnsembl, Reactomes, TractabilityWithId}
import io.opentargets.etl.backend.spark.Helpers.LocationAndSource
import io.opentargets.etl.backend.facetSearch.Helpers._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, collect_set, lit, typedLit, when}
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
      getRelevantDataset[TractabilityWithId](targetsDF, "id", "ensemblGeneId", "tractability")
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
      .distinct()
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
      getRelevantDataset[SubcellularLocationWithId](targetsDF,
                                                    "id",
                                                    "ensemblGeneId",
                                                    "subcellularLocations"
      )
    val subcellularLocationsFacets: Dataset[Facets] = subcellularLocationWithId
      .flatMap(row =>
        row.subcellularLocations.map(s =>
          (row.ensemblGeneId, s.location, "Subcellular Location", s.termSl)
        )
      )
      .toDF("id", "label", "category", "datasourceId")
      .groupBy("label", "category", "datasourceId")
      .agg(collect_set("id").as("entityIds"))
      .distinct()
      .as[Facets]
    subcellularLocationsFacets
  }

  def computeTargetClassFacets(targetsDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing target class facets")
    val targetClassWithId: Dataset[TargetClassWithId] =
      getRelevantDataset[TargetClassWithId](targetsDF, "id", "ensemblGeneId", "targetClass")
    val targetClassFacets: Dataset[Facets] = targetClassWithId
      .flatMap(row => row.targetClass.map(t => (row.ensemblGeneId, t.label, "ChEMBL Target Class")))
      .toDF("ensemblGeneId", "label", "category")
      .groupBy("label", "category")
      .agg(collect_set("ensemblGeneId").as("entityIds"))
      .withColumn("datasourceId", lit(null).cast("string"))
      .distinct()
      .as[Facets]
    targetClassFacets
  }

  def computePathwaysFacets(targetsDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing pathway facets")
    val pathwaysWithId: Dataset[Reactomes] =
      getRelevantDataset[Reactomes](targetsDF, "id", "id", "pathways")
    val pathwaysFacets: Dataset[Facets] = pathwaysWithId
      .flatMap(row => row.pathways.map(p => (row.id, p.pathway, "Reactome", p.pathwayId)))
      .toDF("ensemblGeneId", "label", "category", "datasourceId")
      .groupBy("label", "category", "datasourceId")
      .agg(collect_set("ensemblGeneId").as("entityIds"))
      .distinct()
      .as[Facets]
    pathwaysFacets
  }

  def computeGOFacets(targetsDF: DataFrame, goDF: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[Facets] = {
    import sparkSession.implicits._
    logger.info("Computing GO facets")
    val goAspectMappings: Column = typedLit(
      Map(
        "F" -> "GO:MF",
        "P" -> "GO:BP",
        "C" -> "GO:CC"
      )
    )
    val goWithId: Dataset[GeneOntologyByEnsembl] =
      getRelevantDataset[GeneOntologyByEnsembl](targetsDF, "id", "ensemblId", "go")
    val goFacets: Dataset[Facets] = goWithId
      .flatMap(row => row.go.map(g => (row.ensemblId, g.id, g.aspect)))
      .toDF("ensemblGeneId", "id", "category")
      .join(goDF, Seq("id"), "left")
      .withColumn("label", col("name"))
      .withColumn("datasourceId", col("id"))
      .groupBy("label", "category", "datasourceId")
      .agg(collect_set("ensemblGeneId").as("entityIds"))
      .withColumn("category",
                  when(goAspectMappings($"category").isNotNull, goAspectMappings($"category"))
                    .otherwise($"category")
      )
      .distinct()
      .as[Facets]
    goFacets
  }
}
