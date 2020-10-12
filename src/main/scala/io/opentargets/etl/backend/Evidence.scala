package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.{Helpers => H}

object Evidence extends LazyLogging {
  def evidenceOper(df: DataFrame): DataFrame = {
    val transformations = Map(
      "disease_id" -> col("disease.id"),
      "disease_from_original" -> col("disease.source_name"),
      "disease_from_trait" -> col("disease.reported_trait"),
      "target_id" -> col("target.id"),
      "reported_accession" -> col("accession"),
      "drug_id" -> H.stripIDFromURI(col("drug.id")),
      "row_score"-> col("scores.association_score"),
      "row_id" -> col("id")
    )

    val tdf = transformations.foldLeft(df) {
      case (z, (name, oper)) => z.withColumn(name, oper)
    }

    tdf.selectExpr(transformations.keys.toSeq:_*)
  }

  def compute()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession

    val commonSec = context.configuration.common

    val mappedInputs = Map(
      "evidences" -> H.IOResourceConfig(
        commonSec.inputs.evidence.format,
        commonSec.inputs.evidence.path
      )
    )
    val dfs = H.readFrom(mappedInputs)

    Map("processedEvidences" -> dfs("evidences").transform(evidenceOper))
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    val commonSec = context.configuration.common

    val processedEvidences = compute()

    val outputs = processedEvidences.keys map (name =>
      name -> H.IOResourceConfig(commonSec.outputFormat, commonSec.output + s"/$name"))

    H.writeTo(outputs.toMap, processedEvidences)
  }
}
