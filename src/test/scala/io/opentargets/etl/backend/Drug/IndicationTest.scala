package io.opentargets.etl.backend.Drug

import io.opentargets.etl.backend.Drug.IndicationTest.IndicationRow
import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.drug.Indication
import io.opentargets.etl.backend.spark.Helpers
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object IndicationTest {

  def indicationDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/indication_test30.jsonl").getPath)
  def efoDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/efo_sample.json.gz").getPath)
  case class IndicationRow(id: String, efo_id: String, max_phase_for_indications: Int)
}

class IndicationTest extends EtlSparkUnitTest {

  val getEfoDataFrame: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('getEfoDataframe)
  val approvedIndications: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('approvedIndications)

  import sparkSession.implicits._

  "Processing EFO metadata" should "return a dataframe with the EFO's id, label" in {
    // given
    val inputDF: DataFrame = IndicationTest.efoDf(sparkSession)
    val expectedColumns = Set("updatedEfo", "efoName", "allEfoIds")
    // when
    val results: DataFrame = Indication invokePrivate getEfoDataFrame(inputDF)

    // then
    assert(
      results.columns.forall(expectedColumns.contains),
      s"Expected columns $expectedColumns but found ${results.columns.mkString("Array(", ", ", ")")}"
    )
  }

  "Processing ChEMBL indications data" should "correctly process raw ChEMBL data into expected format" in {
    // given
    val indicationDf: DataFrame = IndicationTest.indicationDf(sparkSession)
    val efoDf: DataFrame = IndicationTest.efoDf(sparkSession)

    // when
    val results: DataFrame = Indication(indicationDf, efoDf)(sparkSession)
    // then
    val expectedColumns: Set[String] =
      Set("id", "indications", "approvedIndications", "indicationCount")

    assert(
      results.columns
        .forall(expectedColumns.contains) && expectedColumns.size == results.columns.length,
      s"All expected columns were expected but instead got ${results.columns.mkString("Array(", ", ", ")")}"
    )
  }

  it should "not contain any entries with no linked disease (efo)" in {
    // given
    val indicationDf: DataFrame = IndicationTest.indicationDf
    val efoDf: DataFrame = IndicationTest.efoDf

    val indication: DataFrame = Indication(indicationDf, efoDf)
    // when
    val results: DataFrame = indication
      .withColumn("inds", explode(col("indications")))
      .select(col("inds.disease").as("efoId"))
      .filter(col("efoId").isNull)
    // then
    assert(results.count() == 0L)
  }

}
