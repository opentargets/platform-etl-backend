package io.opentargets.etl.backend.HpoTest

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.Hpo
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
}

class HpoTest extends EtlSparkUnitTest {

  val getEfoDataFrame: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('getEfoDataframe)
  //val approvedIndications: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('approvedIndications)
  import sparkSession.implicits._

  "Processing EFO metadata" should "return a dataframe with the EFO's disease, name and dbXRefId" in {
    // given
    val inputDF: DataFrame = HpoTest.efoDf(sparkSession)
    val expectedColumns = Set("dbXRefId", "name", "disease")
    // when
    val results: DataFrame = Hpo invokePrivate getEfoDataFrame(inputDF)
    // then
    assert(results.columns.forall(expectedColumns.contains))
  }

}
