package io.opentargets.etl.backend.DrugBeta

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.DrugBeta.IndicationTest.IndicationRow
import io.opentargets.etl.backend.SparkSessionSetup
import io.opentargets.etl.backend.drug_beta.Indication
import io.opentargets.etl.backend.spark.Helpers
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object IndicationTest {

  def indicationDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/indication_test30.jsonl").getPath)
  def efoDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.parquet(this.getClass.getResource("/efo_sample.parquet").getPath)
  case class IndicationRow(id: String, efo_id: String, max_phase_for_indications: Int)
}

class IndicationTest extends EtlSparkUnitTest {

  val getEfoDataFrame: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('getEfoDataframe)
  val approvedIndications: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('approvedIndications)
  import sparkSession.implicits._

  "Approved indications" should "filter indications with phase 4 clinical trials" in {
    // given
    val df: DataFrame = Seq(
      IndicationRow("CHEMBL1", "EFO_1", 2),
      IndicationRow("CHEMBL1", "EFO_2", 2),
      IndicationRow("CHEMBL1", "EFO_3", 4),
      IndicationRow("CHEMBL1", "EFO_4", 4),
    ).toDF
    // when
    val results = Indication invokePrivate approvedIndications(df)
    // then
    assertResult(2)(results.select(explode(col("approvedIndications"))).count)
  }

  "Processing EFO metadata" should "return a dataframe with the EFO's id, label and uri" in {
    // given
    val inputDF: DataFrame = IndicationTest.efoDf(sparkSession)
    val expectedColumns = Set("efo_id", "efo_label", "efo_uri")
    // when
    val results
      : DataFrame = Indication invokePrivate getEfoDataFrame(
      inputDF)

    // then
    results.columns.forall(expectedColumns.contains)
  }

  "The EFO ID" should "be extracted from the URI" in {
    // given
    val inputs = Seq(
      ("http://www.ebi.ac.uk/efo/EFO_1002015", "EFO_1002015"),
      ("http://www.ebi.ac.uk/efo/EFO_1000777", "EFO_1000777"),
      ("http://www.ebi.ac.uk/efo/EFO_1001239", "EFO_1001239"),
      ("http://purl.obolibrary.org/obo/MONDO_0015857", "MONDO_0015857"),
      ("http://www.ebi.ac.uk/efo/EFO_0008622", "EFO_0008622"),
      ("http://www.ebi.ac.uk/efo/EFO_1000237", "EFO_1000237"),
      ("http://www.ebi.ac.uk/efo/EFO_0003859", "EFO_0003859"),
      ("http://www.ebi.ac.uk/efo/EFO_1000232", "EFO_1000232"),
      ("http://www.ebi.ac.uk/efo/EFO_1000165", "EFO_1000165"),
      ("http://www.ebi.ac.uk/efo/EFO_1000134", "EFO_1000134"),
      ("http://www.ebi.ac.uk/efo/EFO_0008560", "EFO_0008560"),
      ("http://purl.obolibrary.org/obo/MONDO_0002654", "MONDO_0002654"),
      ("http://www.ebi.ac.uk/efo/EFO_1000525", "EFO_1000525"),
      ("http://www.ebi.ac.uk/efo/EFO_1000202", "EFO_1000202"),
      ("http://www.ebi.ac.uk/efo/EFO_0007442", "EFO_0007442"),
      ("http://www.ebi.ac.uk/efo/EFO_0003110", "EFO_0003110"),
      ("http://www.ebi.ac.uk/efo/EFO_1001331", "EFO_1001331"),
      ("http://www.ebi.ac.uk/efo/EFO_0009524", "EFO_0009524"),
      ("http://www.ebi.ac.uk/efo/EFO_0009469", "EFO_0009469"),
      ("http://purl.obolibrary.org/obo/MONDO_0008315", "MONDO_0008315")
    )
    val s = StructType(StructField("case", StringType) :: Nil)
    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(inputs.map(_._1).map(Row(_))),
      s)
    // when
    val results = df.withColumn("efo_id", Helpers.stripIDFromURI(col("case")))
    // then
    val expectedResultSet: Set[String] = inputs.map(_._2).toSet
    assert(
      results
        .select("efo_id")
        .map(r => r.getString(0))
        .collect
        .toList
        .forall(expectedResultSet.contains))
  }

  "Processing ChEMBL indications data" should "correctly process raw ChEMBL data into expected format" in {
    // given
    val indicationDf: DataFrame = IndicationTest.indicationDf(sparkSession)
    val efoDf: DataFrame = IndicationTest.efoDf(sparkSession)

    // when
    val results: DataFrame = Indication(indicationDf, efoDf)(sparkSession)
    // then
    val expectedColumns: Set[String] = Set("id", "indications", "approvedIndications")

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
    val results: DataFrame = indication.select(
      col("indications.rows.disease").as("efoId"))
      .filter(col("efoId").isNull)
    // then
    assert(results.count() == 0L)
  }

  "The Indication class" should "correctly replace : with _ in efo ids" in {
    val formatEfoIdsPM: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('formatEfoIds)
    import sparkSession.implicits._
    // given
    case class EfoId(efo_id: String)
    val data = Seq("EFO:0002618", "EFO:0003716", "EFO:0004232").map(Row(_))
    val df: DataFrame =
      sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data),
                                   StructType(StructField("efo_id", StringType) :: Nil))
    // when
    val results
      : DataFrame = Indication invokePrivate formatEfoIdsPM(
      df)
    // then
    assert(
      results
        .select("efo_id")
        .map(r => r.getString(0))
        .collect
        .toList
        .forall(!_.contains(':'))
    )
  }

}
