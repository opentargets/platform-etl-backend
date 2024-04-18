package io.opentargets.etl.backend.facetSearch

import io.opentargets.etl.backend.target.TractabilityWithId
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, collect_set, lit, map_values, typedLit, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object TargetFacets extends LazyLogging {
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
    val tractabilityFacets = tractabilityWithId
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
}
