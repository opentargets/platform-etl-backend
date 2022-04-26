package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.EnsemblTest.ensemblRawDf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object EnsemblTest {
  def ensemblRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/target/homo_test.jsonl.gz").getPath)
}

class EnsemblTest extends EtlSparkUnitTest {

  "Ensembl" should "convert raw dataframe into Ensembl objects without loss" in {
    // given
    val df = ensemblRawDf
    val results = Ensembl(df)
    // then
    results.count should equal(ensemblRawDf.count +- 10)
  }

  "The approved name" should "be extracted from the description field" in {
    // given
    val input = Seq(
      (
        "ENSG00000210049",
        "mitochondrially encoded tRNA-Phe (UUU/C) [Source:HGNC Symbol;Acc:HGNC:7481]]"
      ),
      ("ENSG00000211459", "mitochondrially encoded 12S rRNA [Source:HGNC Symbol;Acc:HGNC:7470]]"),
      (
        "ENSG00000210077",
        "mitochondrially encoded tRNA-Val (GUN) [Source:HGNC Symbol;Acc:HGNC:7500]]"
      ),
      ("ENSG00000210082", "mitochondrially encoded 16S rRNA [Source:HGNC Symbol;Acc:HGNC:7471]]"),
      (
        "ENSG00000209082",
        "mitochondrially encoded tRNA-Leu (UUA/G) 1 [Source:HGNC Symbol;Acc:HGNC:7490]]"
      ),
      (
        "ENSG00000198888",
        "mitochondrially encoded NADH:ubiquinone oxidoreductase core subunit 1 [Source:HGNC Symbol;Acc:HGNC:7455]]"
      ),
      (
        "ENSG00000210100",
        "mitochondrially encoded tRNA-Ile (AUU/C) [Source:HGNC Symbol;Acc:HGNC:7488]]"
      ),
      (
        "ENSG00000210107",
        "mitochondrially encoded tRNA-Gln (CAA/G) [Source:HGNC Symbol;Acc:HGNC:7495]]"
      ),
      (
        "ENSG00000210112",
        "mitochondrially encoded tRNA-Met (AUA/G) [Source:HGNC Symbol;Acc:HGNC:7492]]"
      ),
      (
        "ENSG00000198763",
        "mitochondrially encoded NADH:ubiquinone oxidoreductase core subunit 2 [Source:HGNC Symbol;Acc:HGNC:7456]]"
      ),
      ("ENSG00000280144", "tec")
    )
    val df = sparkSession.createDataFrame(input).toDF("id", "description")
    val methodUnderTest = PrivateMethod[DataFrame]('descriptionToApprovedName)
    // when
    val results = Ensembl invokePrivate methodUnderTest(df)

    // then
    results.count() should equal(df.count())
    results.columns should contain("approvedName")
    results
      .filter(col("id") === "ENSG00000198763")
      .select("approvedName")
      .head()
      .get(0)
      .toString should include(
      "mitochondrially encoded NADH:ubiquinone oxidoreductase core subunit 2"
    )
  }
}
