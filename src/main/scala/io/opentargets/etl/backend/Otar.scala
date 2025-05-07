package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType

object Otar extends LazyLogging {

  /** @param disease
    *   output of ETL disease step
    * @param otarMeta
    *   metadata about otar projects [ otar_code, project_name, project_status ]
    * @param efoLookup
    *   mapping from otar project to disease [ otar_code, efo_disease_id ]
    * @return
    *   dataframe of [ efo_id, projects [ { otar_code, status, project_name, reference } ... ] ]
    */
  def generateOtarInfo(disease: DataFrame, otarMeta: DataFrame, efoLookup: DataFrame): DataFrame = {

    val df = otarMeta.join(efoLookup, Seq("otar_code"), "left_outer")

    df.withColumnRenamed("efo_disease_id", "efo_code")
      .join(disease, col("efo_code") === col("id"), "inner")
      .withColumn("ancestor", explode(concat(array(col("id")), col("ancestors"))))
      .groupBy(col("ancestor").as("efo_id"))
      .agg(
        collect_set(
          struct(
            col("otar_code").as("otar_code"),
            col("project_status").as("status"),
            col("project_name").as("project_name"),
            col("integrates_in_PPP").cast(BooleanType).as("integrates_data_PPP"),
            concat(lit("http://home.opentargets.org/"), col("otar_code")).as("reference")
          )
        ).as("projects")
      )
  }

  def apply()(implicit context: ETLSessionContext): IOResources = {
    val config = context.configuration.steps.otar
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Executing Otar projects step.")
    logger.debug("Reading Otar projects inputs")

    val inputDataFrames = IoHelpers.readFrom(config.input)

    val otarDF = generateOtarInfo(
      inputDataFrames("diseases").data,
      inputDataFrames("otar-meta").data,
      inputDataFrames("otar-project-to-efo").data
    )

    logger.debug("Writing Otar Projects outputs")

    val dataframesToSave: IOResources = Map(
      "otar_projects" -> IOResource(otarDF, config.output("otar"))
    )
    IoHelpers.writeTo(dataframesToSave)
  }
}
