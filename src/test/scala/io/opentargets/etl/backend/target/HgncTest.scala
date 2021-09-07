package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.HgncTest.hgncRawDf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object HgncTest {
  def hgncRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/target/hgnc_test.jsonl").getPath)
}

class HgncTest extends EtlSparkUnitTest {

  val selectAndRenameFields: PrivateMethod[Dataset[Hgnc]] =
    PrivateMethod[Dataset[Hgnc]]('selectAndRenameFields)

  "HGNC" should "convert raw dataframe into HGCN objects without loss" in {
    // given
    val df = hgncRawDf
    // when
    val results = Hgnc invokePrivate selectAndRenameFields(df, sparkSession)
    // then
    results.count must equal(hgncRawDf.select(col("ensembl_gene_id")).distinct.count)
  }
}
