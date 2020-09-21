package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{pow => powCol}
import better.files.Dsl._
import better.files._
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.opentargets.etl.backend.SparkHelpers.IOResourceConfig
import org.apache.spark.sql.expressions._

import scala.math.pow

object AssociationOTF extends LazyLogging {
  case class FacetLevel(l1: Option[String], l2: Option[String])
  case class ReactomeEntry(id: String, label: String)

  implicit class Helpers(df: DataFrame)(implicit context: ETLSessionContext) {
    implicit val ss = context.sparkSession

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
          when(col(keyCol).isNotNull and col(s"${keyCol}.antibody").isNotNull,
               getPositiveCategories(col(s"${keyCol}.antibody.categories")))
        )
        .withColumn(
          "facet_tractability_smallmolecule",
          when(col(keyCol).isNotNull and col(s"${keyCol}.smallmolecule").isNotNull,
               getPositiveCategories(col(s"${keyCol}.smallmolecule.categories")))
        )
    }

    def computeFacetClasses(keyCol: String): DataFrame = {
      df.withColumn(
        s"${keyCol}",
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
    implicit val ss = context.sparkSession
    import ss.implicits._

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

  def computeFacetReactome(df: DataFrame, keyCol: String, vecCol: String)(
      implicit context: ETLSessionContext): DataFrame = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val reactomeSection = context.configuration.common.inputs.reactome

    val mappedInputs = Map(
      "reactome" -> IOResourceConfig(
        reactomeSection.format,
        reactomeSection.path
      )
    )
    val dfs = SparkHelpers.readFrom(mappedInputs)

    val lutReact = ss.sparkContext.broadcast(
      dfs("reactome")
        .selectExpr("id", "label")
        .as[ReactomeEntry]
        .collect()
        .map(e => e.id -> e.label).toMap)

    val mapLevels = udf((l: Seq[String]) =>
      l match {
        case Seq(_, a, _, b, _*) => FacetLevel(lutReact.value.get(a), lutReact.value.get(b))
        case Seq(_, a, _*)       => FacetLevel(lutReact.value.get(a), None)
        case _                   => FacetLevel(None, None)
    })

    val reacts = dfs("reactome")
      .withColumn("levels",
                  when(size(col("path")) > 0, transform(col("path"), (c: Column) => mapLevels(c))))
      .selectExpr("id", "levels")

    val tempDF = df
      .selectExpr(keyCol, vecCol)
      .withColumn(vecCol + "_tmp", explode_outer(col(vecCol)))
      .join(reacts, reacts("id") === col(vecCol + "_tmp"), "left_outer")
      .groupBy(col(keyCol))
      .agg(array_distinct(flatten(collect_list("levels"))).as(vecCol))

    tempDF
  }

  def compute()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession

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

    val commonSec = context.configuration.common

    val diseases = Disease
      .compute()
      .selectExpr(diseaseColumns: _*)
      .orderBy(col("disease_id").asc)
      .persist()

    val targets = Target
      .compute()
      .selectExpr(targetColumns: _*)
      .orderBy(col("target_id").asc)
      .persist()

    val diseasesFacetTAs = computeFacetTAs(diseases, "disease_id", "name", "therapeuticAreas")
      .withColumnRenamed("therapeuticAreas", "facet_therapeuticAreas")

    val targetsFacetReactome = computeFacetReactome(targets, "target_id", "reactome")
      .withColumnRenamed("reactome", "facet_reactome")

    val finalTargets = targets
      .computeFacetTractability("tractability")
      .computeFacetClasses("facet_classes")
      .join(targetsFacetReactome, Seq("target_id"), "left_outer")
      .drop("tractability", "reactome")

    val mappedInputs = Map(
      "evidences" -> IOResourceConfig(
        commonSec.inputs.evidence.format,
        commonSec.inputs.evidence.path
      )
    )
    val dfs = SparkHelpers.readFrom(mappedInputs)

    val evidenceColumns = Seq(
      "id as row_id",
      "disease.id as disease_id",
      "concat(disease.id, ' ',disease.name) as disease_data",
      "target.id as target_id",
      "concat(target.id, ' ', target.gene_info.name, ' ', target.gene_info.symbol) as target_data",
      "sourceID as datasource_id",
      "`type` as datatype_id",
      "scores.association_score as row_score",
      "unique_association_fields"
    )

    logger.info(s"number of evidences ${dfs("evidences").count()}")

    Map(
      "evidences_aotf" -> dfs("evidences")
        .selectExpr(evidenceColumns: _*)
        .repartition()
        .join(diseasesFacetTAs, Seq("disease_id"), "left_outer")
        .join(finalTargets, Seq("target_id"), "left_outer"))
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    val commonSec = context.configuration.common

    val clickhouseEvidences = compute()

    val outputs = clickhouseEvidences.keys map (name =>
      name -> IOResourceConfig(commonSec.outputFormat, commonSec.output + s"/$name"))

    SparkHelpers.writeTo(outputs.toMap, clickhouseEvidences)
  }
}
