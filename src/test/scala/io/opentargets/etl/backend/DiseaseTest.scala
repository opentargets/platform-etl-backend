package io.opentargets.etl.backend

import org.apache.spark.sql.{DataFrame, SparkSession}

object DiseaseTest {

  def efoDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/ontology-efo_sample.jsonl.gz").getPath)
}

class DiseaseTest extends EtlSparkUnitTest {

  "Processing EFO ontology input file" should "return a dataframe with a specific list of attributes" in {
    // given
    val inputDF: DataFrame = DiseaseTest.efoDf(sparkSession)
    val expectedColumns = Set(
      "id",
      "name",
      "ontology",
      "parents",
      "description",
      "therapeuticAreas",
      "ancestors",
      "descendants",
      "dbXRefs",
      "synonyms"
    )
    // when
    val results: DataFrame = Disease.setIdAndSelectFromDiseases(inputDF)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

}
