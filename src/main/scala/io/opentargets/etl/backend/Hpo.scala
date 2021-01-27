package io.opentargets.etl.backend

import org.apache.spark.SparkConf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config.Config
import better.files._
import better.files.File._
import io.opentargets.etl.backend.Disease.{compute, logger}
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{IOResourceConfig, IOResourceConfs, IOResources, unionDataframeDifferentSchema}


object HpoHelpers {
  implicit class AggregationHelpers(df: DataFrame)(implicit ss: SparkSession) {
    import Configuration._
    import ss.implicits._

    def getMondo(diseaseXRefs: DataFrame): DataFrame = {

      val mondoBase = df
        .where(size(col("phenotypes")) > 0)
        .withColumn("phenotypeId", explode(col("phenotypes")))
        .filter(col("phenotypeId").startsWith("HP"))
        .filter(col("id").startsWith(lit("MONDO")))
        .withColumn("newId", regexp_replace(col("id"), "_", ":"))
        .withColumnRenamed("name", "mondoName")
        .withColumn("qualifierNot", lit(false))


      val mondoDiseaseMapping = mondoBase.join(diseaseXRefs, col("dbXRefId") === col("newId"))
        .selectExpr(
          "phenotypeId as phenotype",
          "newId as diseaseFromSourceId",
          "mondoName as diseaseFromSource",
          "name as diseaseName",
          "disease",
          "qualifierNot",
          "resource"
        ).distinct

      mondoDiseaseMapping

    }

    def getHpo: DataFrame = {
      df.filter(size(col("namespace")) > 0)
    }

    def getDiseaseHpo(diseaseXRefs: DataFrame): DataFrame = {

      val orphaDiseaseXRefs = diseaseXRefs.select("disease","name").filter(col("id").startsWith("Orphanet"))
        .withColumn("dbXRefId", regexp_replace(col("disease"), "Orphanet_", "ORPHA:"))

      val xRefs = diseaseXRefs.unionByName(orphaDiseaseXRefs)

      val hpoDiseaseMapping =
        xRefs.join(df, col("dbXRefId") === col("databaseId"))
          .withColumn("qualifierNOT", when(col("qualifier").isNull, false).otherwise(true))
          .distinct
          .withColumn("phenotypeId", regexp_replace(col("HPOId"), ":", "_"))
          .selectExpr(
            "phenotypeId as phenotype",
            "aspect",
            "biocuration as bioCuration",
            "databaseId as diseaseFromSourceId",
            "diseaseName as diseaseFromSource",
            "name as diseaseName",
            "evidenceType",
            "frequency",
            "modifiers",
            "onset",
            "qualifier",
            "qualifierNot",
            "references",
            "sex",
            "disease",
            "resource"
          )

      hpoDiseaseMapping

    }
    def createEvidence: DataFrame = {

      df.groupBy("disease", "phenotype")
        .agg(collect_list(
          struct(
            $"aspect",
            $"bioCuration",
            $"diseaseFromSourceId",
            $"diseaseFromSource",
            $"diseaseName",
            $"evidenceType",
            $"frequency",
            $"modifiers",
            $"onset",
            $"qualifier",
            $"qualifierNot",
            $"references",
            $"sex",
            $"resource"
          )
        ).as("evidence"))

    }
  }
}

object Hpo extends LazyLogging {

  /**
    *
    * @param rawEfoData taken from the `disease` input data
    * @return dataframe of `disease,name,dbXRefs`
    */
  private def getEfoDataframe(rawEfoData: DataFrame): DataFrame = {
    rawEfoData.selectExpr("id as disease", "name", "dbXRefs")
      .withColumn("dbXRefId", explode(col("dbXRefs")))
      .withColumnRenamed("id", "disease")
      .select("dbXRefId", "disease", "name")
  }

  def compute()(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss = context.sparkSession
    import ss.implicits._
    import HpoHelpers._

    val hpoConfiguration = context.configuration.hpo

    logger.info("Loading raw inputs for HP and DiseaseHPO step.")
    val mappedInputs = Map(
      "disease" -> hpoConfiguration.diseaseEtl,
      "mondo" -> hpoConfiguration.mondoOntology,
      "hpo"-> hpoConfiguration.hpoOntology,
      "diseasehpo" -> hpoConfiguration.hpoPhenotype,
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)

    val diseaseXRefs = getEfoDataframe(inputDataFrames("disease"))

    val mondo = inputDataFrames("mondo").getMondo(diseaseXRefs)
    val diseasehpo = inputDataFrames("diseasehpo").getDiseaseHpo(diseaseXRefs)
    val unionDiseaseHpo = unionDataframeDifferentSchema(diseasehpo, mondo).createEvidence

    val hpo = inputDataFrames("hpo").getHpo

    Map(
      "diseasehpo" -> unionDiseaseHpo,
      "hpo" -> hpo
    )
  }

  def apply()(implicit context: ETLSessionContext) = {
    implicit val ss = context.sparkSession
    import ss.implicits._

    val hpoInfo = compute()

    logger.info(s"write to ${context.configuration.common.output}/hpo")
    val outputs = context.configuration.hpo.outputs

    val dataframesToSave: Map[String, (DataFrame, IOResourceConfig)] = Map(
      "diseasehpo" -> (hpoInfo("diseasehpo"), outputs.diseaseHpo),
      "hpo" -> (hpoInfo("hpo"), outputs.hpo),
    )

    val ioResources: IOResources = dataframesToSave mapValues (_._1)
    val saveConfigs: IOResourceConfs = dataframesToSave mapValues (_._2)

    Helpers.writeTo(saveConfigs, ioResources)

  }
}
