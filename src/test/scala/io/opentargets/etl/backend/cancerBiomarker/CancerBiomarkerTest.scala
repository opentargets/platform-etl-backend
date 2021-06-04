package io.opentargets.etl.backend.cancerBiomarker

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.cancerBiomarker.CancerBiomarkerTest.{
  cancerBiomarkerRawDF,
  cb_disease,
  cb_source,
  cb_target
}
import io.opentargets.etl.backend.cancerbiomarkers.{CancerBiomarker, CancerBiomarkers}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object CancerBiomarkerTest {
  def cancerBiomarkerRawDF(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read
      .option("header", true)
      .option("sep", "\t")
      .csv(this.getClass.getResource("/cancerBiomarkers/cancerbiomarkers-2018-05-01.tsv").getPath)

  def cb_disease(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(
      this.getClass.getResource("/cancerBiomarkers/cancer_biomarker_disease.jsonl").getPath)

  def cb_source(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(
      this.getClass.getResource("/cancerBiomarkers/cancer_biomarker_source.jsonl").getPath)

  def cb_target(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.parquet(
      this.getClass.getResource("/cancerBiomarkers/targetSubset.parquet").getPath)

}

class CancerBiomarkerTest extends EtlSparkUnitTest {

  "Cancer Biomarkers" should "convert raw dataframes into Cancer biomarker objects" in {
    // given

    // when
    val results: Dataset[CancerBiomarker] =
      CancerBiomarkers.compute(cancerBiomarkerRawDF, cb_source, cb_disease, cb_target)(ctx)
    // then
    results.filter(col("target") === "ENSG00000182054").count() shouldEqual (5)
  }
}
