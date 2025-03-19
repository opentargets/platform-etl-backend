package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}

object KnownDrugsHelpers {
  def aggregateDrugsByOntology(df: DataFrame): DataFrame = {

    val dfDirect = df
      .groupBy(
        col("diseaseId"),
        col("drugId"),
        col("clinicalPhase").as("phase"),
        col("clinicalStatus").as("status"),
        col("targetId")
      )
      .agg(
        array_distinct(flatten(collect_list(col("urls")))).as("urls")
      )

    dfDirect
  }
}

object KnownDrugs extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val conf = context.configuration.steps.knownDrug

    val inputDataFrame = IoHelpers.readFrom(conf.input)

    val dfDirectInfoAnnotated = compute(List("chembl"), inputDataFrame)

    IoHelpers.writeTo(dfDirectInfoAnnotated)
  }

  def compute(datasources: Seq[String], inputs: IOResources)(implicit
      context: ETLSessionContext
  ): IOResources = {
    implicit val ss: SparkSession = context.sparkSession
    import KnownDrugsHelpers._
    import ss.implicits._

    val diseases = broadcast(
      inputs("diseases").data
        .select(
          $"id".as("diseaseId"),
          $"ancestors",
          $"name".as("label")
        )
        .orderBy($"diseaseId".asc)
    )

    val targets = broadcast(
      inputs("targets").data
        .select(
          col("id") as "targetId",
          col("approvedSymbol"),
          col("approvedName"),
          filter(col("targetClass"), x => x.getField("level") === "l1") as "targetClass"
        )
        .withColumn("targetClass", array_distinct(col("targetClass.label")))
        .orderBy(col("targetId").asc)
    )

    val drugs = broadcast(
      inputs("drug").data
        .join(
          inputs("mechanism").data.withColumn("id", explode($"chemblIds")).drop("chemblIds"),
          Seq("id")
        )
        .select(
          $"id".as("drugId"),
          $"name".as("prefName"),
          $"tradeNames",
          $"synonyms",
          $"drugType",
          $"mechanismOfAction",
          $"targetName",
          $"targets"
        )
        .filter(size($"targets") > 0)
        .withColumn("targetId", explode($"targets"))
        .drop("targets")
        .dropDuplicates("drugId", "targetId")
        .orderBy($"drugId".asc, $"targetId".asc)
    )

    val knownDrugsDF = inputs("evidences").data
      .filter($"sourceId" isInCollection datasources)
      .transform(aggregateDrugsByOntology)
      .join(diseases, Seq("diseaseId"))
      .join(targets, Seq("targetId"))
      .join(drugs, Seq("drugId", "targetId"))

    Map(
      "knownDrugs" -> IOResource(
        knownDrugsDF,
        context.configuration.steps.knownDrug.output("known_drugs")
      )
    )
  }
}
