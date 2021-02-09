package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._

import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.{
  IOResource,
  IOResources,
  unionDataframeDifferentSchema
}

object Hpo extends Serializable with LazyLogging {
  private def getEfoDataframe(rawEfoData: DataFrame): DataFrame = {
    rawEfoData
      .selectExpr("id as disease", "name", "dbXRefs")
      .withColumn("dbXRefId", explode(col("dbXRefs")))
      .withColumnRenamed("id", "disease")
      .select("dbXRefId", "disease", "name")
  }

  def getMondo(mondo: DataFrame, diseaseXRefs: DataFrame): DataFrame = {
    val mondoBase = mondo
      .where(size(col("phenotypes")) > 0)
      .withColumn("phenotypeId", explode(col("phenotypes")))
      .filter(col("phenotypeId").startsWith("HP"))
      .filter(col("id").startsWith(lit("MONDO")))
      .withColumn("newId", regexp_replace(col("id"), "_", ":"))
      .withColumnRenamed("name", "mondoName")
      .withColumn("qualifierNot", lit(false))
    val mondoDiseaseMapping = mondoBase
      .join(diseaseXRefs, col("dbXRefId") === col("newId"))
      .selectExpr(
        "phenotypeId as phenotype",
        "newId as diseaseFromSourceId",
        "mondoName as diseaseFromSource",
        "name as diseaseName",
        "disease",
        "qualifierNot",
        "resource"
      )
      .distinct
    mondoDiseaseMapping
  }

  def getHpo(hpo: DataFrame): DataFrame = hpo.filter(size(col("namespace")) > 0)

  def getDiseaseHpo(diseaseHpoDF: DataFrame, diseaseXRefs: DataFrame): DataFrame = {

    val orphaDiseaseXRefs = diseaseXRefs
      .select("disease", "name")
      .filter(col("id").startsWith("Orphanet"))
      .withColumn("dbXRefId", regexp_replace(col("disease"), "Orphanet_", "ORPHA:"))
    val xRefs = diseaseXRefs.unionByName(orphaDiseaseXRefs)
    val hpoDiseaseMapping =
      xRefs
        .join(diseaseHpoDF, col("dbXRefId") === col("databaseId"))
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

  def createEvidence(diseaseHpo: DataFrame): DataFrame = {

    diseaseHpo
      .groupBy("disease", "phenotype")
      .agg(
        collect_list(
          struct(
            col("aspect"),
            col("bioCuration"),
            col("diseaseFromSourceId"),
            col("diseaseFromSource"),
            col("diseaseName"),
            col("evidenceType"),
            col("frequency"),
            col("modifiers"),
            col("onset"),
            col("qualifier"),
            col("qualifierNot"),
            col("references"),
            col("sex"),
            col("resource")
          )
        ).as("evidence")
      )
  }

  def apply(diseasesRaw: DataFrame)(implicit context: ETLSessionContext): Map[String, DataFrame] = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Loading raw inputs for HP and DiseaseHPO step.")
    val hpoConfiguration = context.configuration.disease

    val mappedInputs = Map(
      "mondo" -> hpoConfiguration.mondoOntology,
      "hpo" -> hpoConfiguration.hpoOntology,
      "diseasehpo" -> hpoConfiguration.hpoPhenotype
    )
    val inputDataFrames = Helpers.readFrom(mappedInputs)

    val diseaseXRefs = getEfoDataframe(diseasesRaw)
    val mondo = getMondo(inputDataFrames("mondo").data, diseaseXRefs)
    val diseasehpo = getDiseaseHpo(inputDataFrames("diseasehpo").data, diseaseXRefs)
    val unionDiseaseHpo = unionDataframeDifferentSchema(diseasehpo, mondo)
    val diseaseHpoEvidence = createEvidence(unionDiseaseHpo)
    val hpo = getHpo(inputDataFrames("hpo").data)

    Map(
      "diseasehpo" -> diseaseHpoEvidence,
      "hpo" -> hpo
    )
  }
}

object Disease extends Serializable with LazyLogging {

  def setIdAndSelectFromDiseases(df: DataFrame): DataFrame = {

    val efosSummary = df
      .withColumn(
        "ancestors",
        array_except(
          array_distinct(flatten(col("path_codes"))),
          array(col("id"))
        )
      )

    val descendants = efosSummary
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor", explode(concat(array(col("id")), col("ancestors"))))
      .groupBy("ancestor")
      .agg(collect_set(col("id")).as("descendants"))
      .withColumnRenamed("ancestor", "id")
      .withColumn(
        "descendants",
        array_except(
          col("descendants"),
          array(col("id"))
        )
      )

    val efos = efosSummary
      .join(descendants, Seq("id"), "left")

    val efosRenamed = efos
      .withColumnRenamed("label", "name")
      .withColumnRenamed("definition", "description")
      .withColumnRenamed("therapeutic_codes", "therapeuticAreas")
      .withColumnRenamed("obsolete_terms", "obsoleteTerms")
      .drop("path_codes", "definition_alternatives", "therapeutic_codes")

    efosRenamed

  }

  // Public because it used by connection.scala
  def compute()(implicit context: ETLSessionContext): DataFrame = {
    implicit val ss: SparkSession = context.sparkSession

    val diseaseConfiguration = context.configuration.disease

    logger.info("Loading raw inputs for Disease step.")
    val mappedInputs = Map(
      "disease" -> diseaseConfiguration.efoOntology
    )

    val inputDataFrames = Helpers.readFrom(mappedInputs)

    val diseaseDF = setIdAndSelectFromDiseases(inputDataFrames("disease").data)

    diseaseDF
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("transform disease dataset")
    // compute is mandatory for running Connection.
    val diseaseDF = compute()

    val hposDF = Hpo(diseaseDF)

    val outputs = context.configuration.disease.outputs
    logger.info(s"write to ${context.configuration.common.output}/disease")
    val dataframesToSave = Map(
      "disease" -> IOResource(diseaseDF, outputs.diseases),
      "diseasehpo" -> IOResource(hposDF("diseasehpo"), outputs.diseaseHpo),
      "hpo" -> IOResource(hposDF("hpo"), outputs.hpo)
    )

    Helpers.writeTo(dataframesToSave)
  }
}
