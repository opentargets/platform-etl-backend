package io.opentargets.etl.backend.Drug

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.drug.Drug
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers._

class DrugTest extends EtlSparkUnitTest {

  "Linked Targets" should "be extracted and grouped from precomputed Mechanisms of Action" in {
    // given
    import sparkSession.implicits._
    val moaDf: DataFrame = Seq(
      (Seq("CHEMBL101253"), Seq("ENSG00000113721", "ENSG00000134853")),
      (Seq("CHEMBL101253"), Seq("ENSG00000182578")),
      (Seq("CHEMBL101253"), Seq("ENSG00000102755", "ENSG00000037280", "ENSG00000128052")),
      (Seq("CHEMBL101"), Seq("ENSG00000073756", "ENSG00000095303")),
      (Seq("CHEMBL101253"), Seq("ENSG00000157404"))
    ).toDF("chemblIds", "targets")
    // when
    val results = Drug.computeLinkedTargets(moaDf).cache
    // then
    results.columns should contain theSameElementsAs List("id", "linkedTargets")
    results.count should be(2) // two unique drug Ids
    results
      .filter('id === "CHEMBL101253")
      .select(explode(col("linkedTargets.rows")))
      .count should be(7)

  }

}
