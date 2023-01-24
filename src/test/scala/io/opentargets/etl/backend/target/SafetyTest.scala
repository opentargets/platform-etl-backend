package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SafetyTest extends EtlSparkUnitTest {

  import sparkSession.implicits._

  def safetyRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/target/safety_100.jsonl").getPath)

  val ensgLookup: DataFrame = Seq(
    ("ENSG1", Seq("ALPP")),
    ("ENSG2", Seq("AR")),
    ("ENSG3", Seq("PGR"))
  ).toDF("ensgId", "name")

  "Toxcast safety entries" should "be mapped to an ENSG" in {
    // given
    val inputs = safetyRawDf.filter(col("datasource") === "ToxCast")
    // when
    val results = Safety.addMissingGeneIdsToSafetyEvidence(inputs, ensgLookup)
    // then
    results.filter(col("id").isNotNull).count() should equal(ensgLookup.count())
  }

  "Safety dataset" should "be properly created" in {
    // given
    val safetyDf = safetyRawDf
    val lookupDf = ensgLookup
    // when
    val results = Safety(safetyEvidence = safetyDf, geneToEnsgLookup = lookupDf)

    // then
    results.count() should be > (10L)
  }
}
