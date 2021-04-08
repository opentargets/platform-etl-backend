package io.opentargets.etl.backend

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.{IOResource, IOResources}
import spark.{Helpers => H}

object AssociationOTF extends LazyLogging {
  case class FacetLevel(l1: Option[String], l2: Option[String])
  case class ReactomeEntry(id: String, label: String)

  implicit class Helpers(df: DataFrame)(implicit context: ETLSessionContext) {
    implicit val ss: SparkSession = context.sparkSession

    def computeFacetTractability(keyCol: String): DataFrame = {
      val getPositiveCategories = udf((r: Row) => {
        if (r != null) {
          Some(
            r.schema.names
              .map(name => if (r.getAs[Double](name) > 0) Some(name) else None)
              .withFilter(_.isDefined)
              .map(_.get))
        } else None
      })

      df.withColumn(
          "facet_tractability_antibody",
          when(col(keyCol).isNotNull and col(s"$keyCol.antibody").isNotNull,
               getPositiveCategories(col(s"$keyCol.antibody.categories")))
        )
        .withColumn(
          "facet_tractability_smallmolecule",
          when(col(keyCol).isNotNull and col(s"$keyCol.smallmolecule").isNotNull,
               getPositiveCategories(col(s"$keyCol.smallmolecule.categories")))
        )
    }

    def computeFacetClasses(keyCol: String): DataFrame = {
      df.withColumn(
        s"$keyCol",
        array_distinct(
          transform(col(keyCol),
                    el =>
                      struct(el.getField("l1")
                               .getField("label")
                               .cast(StringType)
                               .as("l1"),
                             el.getField("l2")
                               .getField("label")
                               .cast(StringType)
                               .as("l2"))))
      )
    }
  }

  def computeFacetTAs(df: DataFrame, keyCol: String, labelCol: String, vecCol: String)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession

    val taID = vecCol + "_tmp"
    val tas = df
      .selectExpr(keyCol, vecCol)
      .withColumn(taID, explode_outer(col(vecCol)))
      .drop(vecCol)

    val labels = df
      .selectExpr(keyCol, labelCol)
      .withColumnRenamed(keyCol, taID)

    tas
      .join(labels, Seq(taID), "left_outer")
      .groupBy(col(keyCol))
      .agg(collect_set(col(labelCol)).as(vecCol))
  }

  def computeFacetReactome(
      targets: DataFrame,
      keyCol: String,
      vecCol: String,
      reactomeDF: DataFrame)(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession
    import ss.implicits._

    val lutReact = ss.sparkContext.broadcast(
      reactomeDF
        .selectExpr("id", "label")
        .as[ReactomeEntry]
        .collect()
        .map(e => e.id -> e.label)
        .toMap)

    val mapLevels = udf((l: Seq[String]) =>
      l match {
        case Seq(a, _, b, _*) => FacetLevel(lutReact.value.get(a), lutReact.value.get(b))
        case Seq(a, _*)       => FacetLevel(lutReact.value.get(a), None)
        case _                => FacetLevel(None, None)
    })

    val reacts = reactomeDF
      .withColumn("levels",
                  when(size(col("path")) > 0, transform(col("path"), (c: Column) => mapLevels(c))))
      .selectExpr("id", "levels")

    val tempDF = targets
      .selectExpr(keyCol, vecCol)
      .withColumn(vecCol + "_tmp", explode_outer(col(vecCol)))
      .join(reacts, reacts("id") === col(vecCol + "_tmp"), "left_outer")
      .groupBy(col(keyCol))
      .agg(array_distinct(flatten(collect_list("levels"))).as(vecCol))

    tempDF
  }

  def compute()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val conf = context.configuration
    val mappedInputs = Map(
      "evidences" -> conf.aotf.inputs.evidences,
      "targets" -> conf.aotf.inputs.targets,
      "diseases" -> conf.aotf.inputs.diseases,
      "reactome" -> conf.aotf.inputs.reactome
    )

    val dfs = H.readFrom(mappedInputs)

    val diseaseColumns = Seq(
      "id as disease_id",
      "therapeuticAreas",
      "name"
    )

    val targetColumns = Seq(
      "id as target_id",
      "proteinAnnotations.classes as facet_classes",
      "reactome",
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

    val diseasesFacetTAs = computeFacetTAs(diseases, "disease_id", "name", "therapeuticAreas")
      .withColumnRenamed("therapeuticAreas", "facet_therapeuticAreas")

    val targetsFacetReactome =
      computeFacetReactome(targets, "target_id", "reactome", dfs("reactome").data)
        .withColumnRenamed("reactome", "facet_reactome")

    val finalTargets = targets
      .computeFacetTractability("tractability")
      .computeFacetClasses("facet_classes")
      .join(targetsFacetReactome, Seq("target_id"), "left_outer")
      .drop("tractability", "reactome")

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
      "concat(diseaseId, ' ',diseaseLabel) as disease_data",
      "targetId as target_id",
      "concat(targetId, ' ', targetName, ' ', targetSymbol) as target_data",
      "datasourceId as datasource_id",
      "datatypeId as datatype_id",
      "score as row_score"
    )

    val elasticsearchDF = dfs("evidences").data
      .drop(columnsToDrop: _*)
      .selectExpr(evidenceColumns: _*)
      .join(diseasesFacetTAs, Seq("disease_id"), "left_outer")
      .join(finalTargets, Seq("target_id"), "left_outer")

    val clickhouseDF = dfs("evidences").data
      .drop(columnsToDrop: _*)
      .selectExpr(evidenceColumns: _*)

    Map(
      "aotfsElasticsearch" -> IOResource(elasticsearchDF, conf.aotf.outputs.elasticsearch),
      "aotfsClickhouse" -> IOResource(clickhouseDF, conf.aotf.outputs.clickhouse)
    )
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    val clickhouseEvidences = compute()

    H.writeTo(clickhouseEvidences)
  }
}
