package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.{Helpers => H}

object Evidence extends LazyLogging {
  def evidenceOper(df: DataFrame): DataFrame = {
    val transformations = Map(
      "diseaseId" -> col("disease.id"),
      "diseaseFromOriginal" -> col("disease.source_name"),
      "diseaseFromTrait" -> col("disease.reported_trait"),
      "targetId" -> col("target.id"),
      "reportedAccession" -> col("accession"),
      "drugId" -> H.stripIDFromURI(col("drug.id")),
      "rowScore"-> col("scores.association_score"),
      "rowId" -> col("id"),
      "variantId" -> col("variant.id"),
      "rsId" -> col("variant.rs_id"),
      "allelicComposition" -> col("allelic_composition"),
    )

    //  |-- variant: struct (nullable = true)
    // |    |-- id: string (nullable = true)
    // |    |-- rs_id: string (nullable = true)
    // |    |-- source_link: string (nullable = true)
    // |    |-- type: string (nullable = true)
    // |    |-- type_link: string (nullable = true)
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
