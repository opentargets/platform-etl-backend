package io.opentargets.etl.backend.HpoTest

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.Hpo
import io.opentargets.etl.backend.HpoHelpers
import io.opentargets.etl.backend.spark.Helpers
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// cat efo_sample.json | jq .id > ids.json
// cat out/diseases/part* |  jq -c -n --slurpfile ids ids.json 'inputs | . as $in | select( $ids | index($in.id))' > new_efo.json
// shuf -n 21412 hpo-phenotypes-sample.jsonl > hpo-pheno.jsonl


object HpoTest {

  def efoDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/efo_sample.json.gz").getPath)

  def mondoDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/mondo_sample.jsonl.gz").getPath)

  def hpoPhenotypesDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/hpo-phenotypes-sample.jsonl.gz").getPath)
}

class HpoTest extends EtlSparkUnitTest {
  import HpoHelpers._

  val getEfoDataFrame: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('getEfoDataframe)
  import sparkSession.implicits._

  "Processing EFO metadata" should "return a dataframe with the EFO's disease, name and dbXRefId" in {
    // given
    val inputDF: DataFrame = HpoTest.efoDf(sparkSession)
    val expectedColumns = Set("dbXRefId", "name", "disease")
    // when
    val results: DataFrame = Hpo invokePrivate getEfoDataFrame(inputDF)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

  "Processing Disease CrossRef and MONDO ontology" should "return a dataframe with phenotype and diseaseFromSourceId" in {
    // given
    // result of before
    val outputDiseaseDF: DataFrame = HpoTest.efoDf(sparkSession)
    val diseaseDF: DataFrame = Hpo invokePrivate getEfoDataFrame(outputDiseaseDF)
    val inputDF: DataFrame = HpoTest.mondoDf(sparkSession)
    val expectedColumns = Set("disease", "resource","diseaseFromSourceId","phenotype", "resource","qualifierNot")
    // when
    val results: DataFrame = inputDF.getMondo(diseaseDF)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

  "Processing Disease CrossRef and phenotype.hpoa " should "return a dataframe with phenotype and diseaseFromSourceId" in {
    // given
    // result of before
    val outputDiseaseDF: DataFrame = HpoTest.efoDf(sparkSession)
    val diseaseDF: DataFrame = Hpo invokePrivate getEfoDataFrame(outputDiseaseDF)
    val inputDF: DataFrame = HpoTest.hpoPhenotypesDf(sparkSession)
    val expectedColumns = Set("disease", "resource","diseaseFromSourceId","phenotype", "resource","qualifierNot")
    // when
    val results: DataFrame = inputDF.getDiseaseHpo(diseaseDF)

    // then
    assert(expectedColumns.forall(expectedCol => results.columns.contains(expectedCol)))
  }

}
