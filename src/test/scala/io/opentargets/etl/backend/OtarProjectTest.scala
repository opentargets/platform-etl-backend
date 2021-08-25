package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.SparkSession

object OtarProjectTest {

  def setupResources(implicit sparkSession: SparkSession): IOResources = {

    val inputDiseases = Seq(
      ("EFO_0003767", List("EFO1", "EFO2")),
      ("EFO_0000729", List("EFO4", "EFO5")),
      ("EFO_0000692", List("EFO1", "EFO3", "EFO7")),
      ("EFO_0000123", List("EFO7", "EFO9", "EFO1"))
    )
    val diseases = sparkSession.createDataFrame(inputDiseases).toDF("id", "ancestors")

    val inputOtarPrj = Seq(
      ("OTAR_x01", "Project Alfa", "Active", "EFO_0000692"),
      ("OTAR_x02", "Project Beta", "Closed", "EFO_0003767"),
      ("OTAR_x04", "Project Gamma", "Active", "EFO_0000729"),
      ("OTAR_x07", "Project Delta", "Closed", "EFO_0000123")
    )
    val otarProjects =
      sparkSession
        .createDataFrame(inputOtarPrj)
        .toDF("otar_code", "project_name", "project_status", "efo_code")

    val config = IOResourceConfig("csv", "")
    val allResources: IOResources =
      Map(
        "diseases" -> IOResource(diseases, config),
        "projects" -> IOResource(otarProjects, config)
      )
    allResources
  }
}

class OtarProjectTest extends EtlSparkUnitTest {
  import sparkSession.implicits._

  "Processing Diseases and Otar Projects" should "return a dataframe with a specific list of attributes" in {
    // given
    val resources: IOResources = OtarProjectTest.setupResources(sparkSession)
    val expectedColumns = Set("efo_id", "projects")
    // when
    val results = OtarProject.generateOtarInfo(resources)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

  "Processing Diseases and Otar Projects" should "return EFO id from ancestors info" in {
    // given
    val resources: IOResources = OtarProjectTest.setupResources(sparkSession)
    val expectedColumns = Set("efo_id", "projects")
    // when
    val results = OtarProject.generateOtarInfo(resources)

    // then
    assert(results.filter(col("efo_id") === "EFO5").count() === 1)
  }
}
