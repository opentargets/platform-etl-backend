package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AssociationOTF extends LazyLogging {

  def computeFacetClasses(df: DataFrame): DataFrame = {
    val fcDF = df
      .select(
        col("target_id"),
        explode(
          filter(
            col("facet_classes"),
            c => {
              val level = c.getField("level")
              level === "l1" || level === "l2"
            }
          )
        ) as "fc"
      )
      .select("target_id", "fc.*")
      .orderBy("target_id", "id", "level")
      .groupBy("target_id", "id")
      .agg(collect_list("label") as "levels")
      .withColumn(
        "facet_classes",
        struct(col("levels").getItem(0) as "l1", col("levels").getItem(1) as "l2")
      )
      .groupBy("target_id")
      .agg(collect_list("facet_classes") as "facet_classes")
      .orderBy("target_id")

    df.drop("facet_classes").join(fcDF, Seq("target_id"), "left_outer")
  }

  def computeFacetTAs(df: DataFrame, keyCol: String, labelCol: String, vecCol: String)(implicit
      context: ETLSessionContext
  ): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession

    val taID = vecCol + "_tmp"

    // [disease_id, therapeuticAreas_tmp]
    val tas = df
      .selectExpr(keyCol, vecCol)
      .withColumn(taID, explode_outer(col(vecCol)))
      .drop(vecCol)

    // [therapeuticAreas_tmp, name]
    val labels = df
      .selectExpr(keyCol, labelCol)
      .withColumnRenamed(keyCol, taID)

    tas
      .join(labels, Seq(taID), "left_outer")
      .groupBy(col(keyCol))
      .agg(collect_set(col(labelCol)).as(vecCol))

  }

  def computeFacetTractability(df: DataFrame): DataFrame = {
    val facetFilter: (Column, String) => Column = (c: Column, name: String) => {
      c.getField("value") === true && c.getField("modality") === name
    }
    val tractabilityFacetsDF = df
      .select(
        col("target_id"),
        filter(col("tractability"), facetFilter(_, "SM")) as "sm",
        filter(col("tractability"), facetFilter(_, "AB")) as "ab",
        filter(col("tractability"), facetFilter(_, "PR")) as "pr",
        filter(col("tractability"), facetFilter(_, "OC")) as "oc",
        monotonically_increasing_id() as "miid"
      )
      .select(
        col("target_id"),
        col("sm.id") as "facet_tractability_smallmolecule",
        col("ab.id") as "facet_tractability_antibody",
        col("pr.id") as "facet_tractability_protac",
        col("oc.id") as "facet_tractability_othermodalities",
        col("miid")
      )
      .orderBy("target_id")

    df.join(tractabilityFacetsDF, Seq("target_id"), "left_outer")
      .orderBy("miid")
      .drop("miid")
  }

  def compute()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val conf = context.configuration
    val mappedInputs = Map(
      "evidences" -> conf.aotf.inputs.evidences,
      "targets" -> conf.aotf.inputs.targets,
      "diseases" -> conf.aotf.inputs.diseases
    )

    val dfs = IoHelpers.readFrom(mappedInputs)

    val diseaseColumns = Seq(
      "id as disease_id",
      "concat(id, ' ',name) as disease_data",
      "therapeuticAreas",
      "name"
    )

    val targetColumns = Seq(
      "id as target_id",
      "concat(id, ' ', approvedName, ' ', approvedSymbol) as target_data",
      "targetClass as facet_classes",
      "pathways as reactome",
      "tractability"
    )

    val diseases = dfs("diseases").data
      .selectExpr(diseaseColumns: _*)
      .orderBy(col("disease_id").asc)
      .persist()

    val targets = dfs("targets").data
      .selectExpr(targetColumns: _*)
      .orderBy(col("target_id").asc)
      .persist()

    val diseasesFacetTAs =
      computeFacetTAs(diseases, "disease_id", "name", "therapeuticAreas")
        .withColumnRenamed("therapeuticAreas", "facet_therapeuticAreas")

    val targetsFacetReactome =
      targets.select(
        col("target_id"),
        transform(
          col("reactome"),
          r => struct(r.getField("topLevelTerm") as "l1", r.getField("pathway") as "l2")
        ) as "facet_reactome"
      )

    val finalTargets = targets
      .transform(computeFacetClasses)
      .transform(computeFacetTractability)
      .join(targetsFacetReactome, Seq("target_id"), "left_outer")
      .drop("reactome", "tractability")

    val finalDiseases = diseases
      .join(diseasesFacetTAs, Seq("disease_id"), "left_outer")
      .drop("therapeuticAreas")

    val columnsToDrop = Seq(
      "mutatedSamples",
      "diseaseModelAssociatedModelPhenotypes",
      "diseaseModelAssociatedHumanPhenotypes",
      "textMiningSentences",
      "clinicalUrls"
    )

    val evidenceColumns = Seq(
      "id as row_id",
      "diseaseId as disease_id",
      "targetId as target_id",
      "datasourceId as datasource_id",
      "datatypeId as datatype_id",
      "score as row_score"
    )

    val evidenceColumnsCleaned = Seq(
      "row_id",
      "disease_id",
      "target_id",
      "datasource_id",
      "datatype_id",
      "row_score",
      "disease_data",
      "target_data"
    )

    val elasticsearchDF = dfs("evidences").data
      .drop(columnsToDrop: _*)
      .selectExpr(evidenceColumns: _*)
      .join(finalDiseases, Seq("disease_id"), "left_outer")
      .join(finalTargets, Seq("target_id"), "left_outer")

    val clickhouseDF = dfs("evidences").data
      .selectExpr(evidenceColumns: _*)
      .join(finalDiseases, Seq("disease_id"), "left_outer")
      .join(finalTargets, Seq("target_id"), "left_outer")
      .selectExpr(evidenceColumnsCleaned: _*)

    Map(
      "aotfsElasticsearch" -> IOResource(elasticsearchDF, conf.aotf.outputs.elasticsearch),
      "aotfsClickhouse" -> IOResource(clickhouseDF, conf.aotf.outputs.clickhouse)
    )
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    val clickhouseEvidences = compute()

    IoHelpers.writeTo(clickhouseEvidences)
  }
}
