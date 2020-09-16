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
  def compute()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import AssociationHelpers._

    val diseaseColumns = Seq(
      "id",
      "therapeuticAreas"
    )

    val targetColumns = Seq(
      "id",
      "classes",
      "reactome",
      "tractability"
    )

    val commonSec = context.configuration.common

    val diseases = Disease.compute().selectExpr(diseaseColumns:_*)
    val targets = Target.compute().selectExpr(targetColumns:_*)

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
      "unique_association_fields.*"
    )

    Map("evidences_aotf" -> dfs("evidences")
      .selectExpr(evidenceColumns:_*)
      .repartition()
    )
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
