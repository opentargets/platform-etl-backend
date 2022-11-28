package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import io.opentargets.etl.backend.spark.{IOResource, IOResourceConfig, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.SparkSession

class OtarProjectTest extends EtlSparkUnitTest {
  import sparkSession.implicits._
  lazy val inputDiseases: DataFrame = Seq(
    ("EFO_0003767", List("EFO1", "EFO2")),
    ("EFO_0000729", List("EFO4", "EFO5")),
    ("EFO_0000692", List("EFO1", "EFO3", "EFO7")),
    ("EFO_0000123", List("EFO7", "EFO9", "EFO1"))
  ).toDF("id", "ancestors")

  lazy val inputOtarMeta: DataFrame = Seq(
    ("OTAR_x01", "Project Alfa", "Active", "true"),
    ("OTAR_x02", "Project Beta", "Closed", "true"),
    ("OTAR_x04", "Project Gamma", "Active", "true"),
    ("OTAR_x07", "Project Delta", "Closed", "true")
  ).toDF("otar_code", "project_name", "project_status", "integrates_in_ppp")

  lazy val inputOtarLookup: DataFrame = Seq(
    ("OTAR_x01", "EFO_0000729"),
    ("OTAR_x01", "EFO_0003767")
  ).toDF("otar_code", "efo_disease_id")

  "Processing Diseases and Otar Projects" should "return a dataframe with a specific list of attributes" in {
    // given
    val expectedColumns = Set("efo_id", "projects")
    // when
    val results = OtarProject.generateOtarInfo(inputDiseases, inputOtarMeta, inputOtarLookup)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

  it should "return EFO id from ancestors info" in {
    // given
    // when
    val results = OtarProject.generateOtarInfo(inputDiseases, inputOtarMeta, inputOtarLookup)

    // then
    assert(results.filter(col("efo_id") === "EFO5").count() === 1)
  }
}
