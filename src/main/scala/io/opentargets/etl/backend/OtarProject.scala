package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OtarProject extends LazyLogging {

  def generateOtarInfo(inputDataFrames: IOResources): DataFrame = {

    inputDataFrames("projects").data
      .join(inputDataFrames("diseases").data, col("efo_code") === col("id"), "inner")
      .withColumn("ancestor", explode(concat(array(col("id")), col("ancestors"))))
      .groupBy(col("ancestor").as("efo_id"))
      .agg(
        collect_set(
          struct(
            col("otar_code").as("otar_code"),
            col("project_status").as("status"),
            col("project_name").as("project_name"),
            concat(lit("http://home.opentargets.org/"), col("otar_code")).as("reference")
          )
        ).as("projects")
      )
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Executing Otar projects step.")

    logger.debug("Reading Otar projects inputs")
    val OtarConfiguration = context.configuration.otarproject

    val mappedInputs = Map(
      "diseases" -> OtarConfiguration.diseaseEtl,
      "projects" -> OtarConfiguration.otar
    )
    val inputDataFrames = IoHelpers.readFrom(mappedInputs)

    val otarDF = generateOtarInfo(inputDataFrames)

    logger.debug("Writing Otar Projects outputs")
    val dataframesToSave: IOResources = Map(
      "otar_projects" -> IOResource(otarDF, context.configuration.otarproject.output)
    )
    IoHelpers.writeTo(dataframesToSave)

  }

}
