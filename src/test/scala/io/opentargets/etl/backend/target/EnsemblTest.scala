package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.EnsemblTest.ensemblRawDf
import org.apache.spark.sql.{DataFrame, SparkSession}

object EnsemblTest {
  def ensemblRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/target/ensembl_test.jsonl").getPath)
}

class EnsemblTest extends EtlSparkUnitTest {
  "Ensembl" should "convert raw dataframe into Ensembl objects without loss" in {
    // given
    val df = ensemblRawDf
    // when
    val results = Ensembl(df)
    // then
    results.count must equal(ensemblRawDf.count)
  }
}
