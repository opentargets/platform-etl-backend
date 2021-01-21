package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{IOResourceConfig, IOResources}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.storage.StorageLevel

object KnownDrugsHelpers {
  def aggregateDrugsByOntology(df: DataFrame)(implicit ss: SparkSession): DataFrame = {

    val dfDirect = df
      .groupBy(
        col("diseaseId"),
        col("drugId"),
        col("clinicalPhase").as("phase"),
        col("clinicalStatus").as("status"),
        col("targetId")
      )
      .agg(
        array_distinct(flatten(collect_list(col("clinicalUrls")))).as("urls")
      )

    dfDirect
  }
}

object KnownDrugs extends LazyLogging {
  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    val conf = context.configuration.knownDrugs
    val mappedInputs = Map(
      "evidence" -> conf.inputs.evidences,
      "disease" -> conf.inputs.diseases,
      "target" -> conf.inputs.targets,
      "drug" -> conf.inputs.drugs.drug,
      "mechanism" -> conf.inputs.drugs.mechanismOfAction
    )
    val inputDataFrame = Helpers.readFrom(mappedInputs)

    val dfDirectInfoAnnotated = compute(List("chembl"), inputDataFrame)

    val outputConfs = dfDirectInfoAnnotated.keys.map { name =>
      name -> IOResourceConfig(context.configuration.common.outputFormat,
                               s"${context.configuration.common.output}/$name")
    }

    Helpers.writeTo(outputConfs.toMap, dfDirectInfoAnnotated)
  }

  def compute(datasources: Seq[String], inputs: Map[String, DataFrame])(
      implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss: SparkSession = context.sparkSession
    import KnownDrugsHelpers._
    import ss.implicits._

    val diseases = inputs("disease")
      .select(
        $"id".as("diseaseId"),
        $"ancestors",
        $"name".as("label")
      )
      .orderBy($"diseaseId".asc)
      .persist(StorageLevel.DISK_ONLY)

    val targets = inputs("target")
      .select(
        $"id".as("targetId"),
        $"approvedSymbol",
        $"approvedName",
        array_distinct(transform(expr("proteinAnnotations.classes"),
                                 c => c.getField("l1").getField("label"))).as("targetClass")
      )
      .orderBy($"targetId".asc)
      .persist(StorageLevel.DISK_ONLY)

    val drugs = inputs("drug")
      .join(inputs("mechanism").withColumn("id", explode($"chemblIds")).drop("chemblIds"),
        Seq("id"))
      .select(
        $"id".as("drugId"),
        $"name".as("prefName"),
        $"tradeNames",
        $"synonyms",
        $"drugType",
        $"mechanismOfAction",
        $"references",
        $"targetName",
        $"targets"
      )
      .filter(size($"targets") > 0)
      .withColumn("targetId", explode($"targets"))
      .drop("targets")
      .dropDuplicates
      .orderBy($"drugId".asc, $"targetId".asc)
      .persist(StorageLevel.DISK_ONLY)

    val knownDrugsDF = inputs("evidence")
      .drop("targetName", "targetSymbol", "diseaseLabel")
      .filter($"sourceId" isInCollection datasources)
      .transform(aggregateDrugsByOntology)
      .join(diseases, Seq("diseaseId"))
      .join(targets, Seq("targetId"))
      .join(drugs, Seq("drugId", "targetId"))

    Map(
      "knownDrugs" -> knownDrugsDF
    )
  }
}
