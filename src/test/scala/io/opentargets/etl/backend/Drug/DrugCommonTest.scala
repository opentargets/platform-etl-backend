package io.opentargets.etl.backend.Drug

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.drug.DrugCommon
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers._

class DrugCommonTest extends EtlSparkUnitTest {

  def chemblEvidenceDF(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/chembl31_evidence_200.jsonl").getPath)
  "Linked targets and diseases" should "be extracted from Chembl evidence file" in {
    // given
    val evidenceDf: DataFrame = chemblEvidenceDF
    // when
    val results = DrugCommon.getUniqTargetsAndDiseasesPerDrugId(evidenceDf)
    // then
    results.columns.toList should contain allElementsOf List("id",
                                                             "linkedTargets",
                                                             "linkedDiseases"
    )
  }
}
