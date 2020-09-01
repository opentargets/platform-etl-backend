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
  def prepareEvidences(expandOntology: Boolean = false)(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import AssociationHelpers._

    val commonSec = context.configuration.common
    val associationsSec = context.configuration.associations

    val mappedInputs = Map(
      "evidences" -> IOResourceConfig(
        commonSec.inputs.evidence.format,
        commonSec.inputs.evidence.path
      )
    )
    val dfs = SparkHelpers.readFrom(mappedInputs)

    val evidenceColumns = Seq(
      "id as row_id",
      "disease.id as A_id",
      "concat(disease.id, ' ',disease.name) as A_data",
      "target.id as B_id",
      "concat(target.id, ' ', target.gene_info.name, ' ', target.gene_info.symbol) as B_data",
      "sourceID as datasource_id",
      "`type` as datatype_id",
      "scores.association_score as row_score",
      "unique_association_fields.*"
    )

    Map("evidences_aotf" -> dfs("evidences")
      .selectExpr(evidenceColumns:_*)
      .where($"row_score" > 0D)
      .repartition()
    )
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    val commonSec = context.configuration.common

    val clickhouseEvidences = prepareEvidences()

    val outputs = clickhouseEvidences.keys map (name =>
      name -> IOResourceConfig(commonSec.outputFormat, commonSec.output + s"/$name"))

    SparkHelpers.writeTo(outputs.toMap, clickhouseEvidences)
  }
}
