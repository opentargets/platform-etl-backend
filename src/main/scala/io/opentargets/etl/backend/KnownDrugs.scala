package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{IOResourceConfig, stripIDFromURI}
import org.apache.spark.storage.StorageLevel

object KnownDrugsHelpers {
    def aggregateDrugsByOntology(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
      import ss.implicits._

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
  def compute(datasources: Seq[String], inputs: Map[String, DataFrame])(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import KnownDrugsHelpers._

    val diseases = inputs("disease")
      .select(
        $"id".as("diseaseId"),
        $"ancestors",
        $"name".as("label")
      ).orderBy($"diseaseId".asc).persist(StorageLevel.DISK_ONLY)

    val targets = inputs("target")
      .select(
        $"id".as("targetId"),
        $"approvedSymbol",
        $"approvedName",
        array_distinct(transform(expr("proteinAnnotations.classes"),c =>
          c.getField("l1").getField("label"))
        ).as("targetClass")
      ).orderBy($"targetId".asc).persist(StorageLevel.DISK_ONLY)

    val drugs = inputs("drug")
      .select(
        $"id".as("drugId"),
        $"name".as("prefName"),
        $"tradeNames",
        $"synonyms",
        $"drugType",
        $"mechanismsOfAction".getField("rows").as("moas")
      )
      .filter(size($"moas") > 0)
      .withColumn("moa", explode($"moas"))
      .select($"*", expr("moa.*"))
      .drop("moas", "moa")
      .filter(size($"targets") > 0)
      .withColumn("targetId", explode($"targets"))
      .drop("targets")
      .orderBy($"drugId".asc, $"targetId".asc).persist(StorageLevel.DISK_ONLY)

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

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import KnownDrugsHelpers._

    val conf = context.configuration.knownDrugs
    val mappedInputs = Map(
      "evidence" -> conf.inputs.evidences,
      "disease" -> conf.inputs.diseases,
      "target" -> conf.inputs.targets,
      "drug" -> conf.inputs.drugs
    )
    val inputDataFrame = Helpers.readFrom(mappedInputs)

    val dfDirectInfoAnnotated = compute(List("chembl"), inputDataFrame)

    val outputConfs = dfDirectInfoAnnotated.keys.map {
      name =>
        name -> IOResourceConfig(
          context.configuration.common.outputFormat,
          s"${context.configuration.common.output}/$name")
    }

    Helpers.writeTo(outputConfs.toMap, dfDirectInfoAnnotated)
  }
}
