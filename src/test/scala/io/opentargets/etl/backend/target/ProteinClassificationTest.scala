package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.ProteinClassificationTest.chemblTargetRawDf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object ProteinClassificationTest {
  def chemblTargetRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/target/chembl_target_100.jsonl.gz").getPath)
}

class ProteinClassificationTest extends EtlSparkUnitTest {
  "Protein classications" should "be extracted from Chembl inputs" in {
    // given
    val inputs = chemblTargetRawDf
    // when
    val results = ProteinClassification.apply(inputs)
    // then
    results.count() should be > 100L // each input has at least one accession.
  }

  they should "only include levels for which there is a label" in {
    // given
    val inputs = chemblTargetRawDf

    // when
    val results = ProteinClassification(inputs)

    // then
    results.filter(row => row.targetClass.map(tc => tc.label).forall(s => s.nonEmpty))
  }
}
