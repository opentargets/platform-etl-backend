package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AssociationOTF extends LazyLogging {
  case class FacetLevel(l1: Option[String], l2: Option[String])
  case class ReactomeEntry(id: String, label: String)

  /* adds columns facet_tractability_antibody and facet_tractability_smallmolecule to dataframe.
  I don't know if this can be done with the new tractability data, and I don't know if it is even
  used.
   */
  def computeFacetTractability(df: DataFrame, keyCol: String): DataFrame = {
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

  /*
  Adds keyCol to dataframe, where keycol is array(struct(l1: String, l2: String)) where l1 comes from l1.label and l2
  comes from l2.label.

  In the new data this is targetClass which is a struct of id, label, level. We'd need to explode and then group by
  targetId, targetClassId and then take the label of levels 1 and 2.
   */
  def computeFacetClasses(df: DataFrame, keyCol: String): DataFrame = {
    df.withColumn(
      keyCol,
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

  def computeFacetTAs(df: DataFrame, keyCol: String, labelCol: String, vecCol: String)(
      implicit context: ETLSessionContext): DataFrame = {
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

  def compute()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val conf = context.configuration
    val mappedInputs = Map(
      "evidences" -> conf.aotf.inputs.evidences,
      "targets" -> conf.aotf.inputs.targets,
      "diseases" -> conf.aotf.inputs.diseases,
      "reactome" -> conf.aotf.inputs.reactome
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
      "targetClass as facet_classes", //fixme: this is now targetClass
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
      targets.withColumn(
        "facet_reactome",
        transform(col("reactome"),
                  r => struct(r.getField("topLevelTerm") as "l1", r.getField("pathway") as "l2")))

    val finalTargets = targets
      .transform(computeFacetTractability(_, "tractability"))
      .transform(computeFacetClasses(_, "facet_classes"))
      .join(targetsFacetReactome, Seq("target_id"), "left_outer")
      .drop("tractability", "reactome")

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
