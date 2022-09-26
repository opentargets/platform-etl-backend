package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.EnsemblTest.ensemblRawDf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.should.Matchers._

object EnsemblTest {
  def ensemblRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/target/homo_test.jsonl.gz").getPath)
}

case class Exon(start: Int, end: Int)

class EnsemblTest extends EtlSparkUnitTest with PrivateMethodTester {

  "Ensembl" should "convert raw dataframe into Ensembl objects without loss" in {
    // given
    import sparkSession.implicits._
    val df = ensemblRawDf
    val results = Ensembl(df, sparkSession.emptyDataset[GeneAndCanonicalTranscript])
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

  "Canonical exons" should "be added as array of (start, stop, start, stop...)" in {
    // given
    import sparkSession.implicits._
    val addExons = PrivateMethod[DataFrame]('addCanonicalExons)
    val df: DataFrame = Seq(
      (CanonicalTranscript("T1", "1", 1, 1, "+"),
       Array("T1", "T2"),
       Array(Array(Exon(1, 2), Exon(5, 6)), Array(Exon(1, 2)))
      ),
      (CanonicalTranscript("T2", "1", 1, 1, "-"),
       Array("T1", "T2"),
       Array(Array(Exon(1, 2), Exon(5, 6)), Array(Exon(3, 5), Exon(7, 9)))
      )
    ).toDF("canonicalTranscript", "transcriptIds", "exons")

    // when
    val result = Ensembl invokePrivate addExons(df)

    // then
    val t1: Seq[Integer] = result
      .filter(col("canonicalTranscript.id") === "T1")
      .select(col("canonicalExons"))
      .head
      .getSeq[Integer](0)
    t1 should contain inOrderOnly (1, 2, 5, 6)

  }

  "Canonical Transcript" should "be on the same chromosome as the Gene" in {
    // given
    import sparkSession.implicits._
    val addTranscript = PrivateMethod[DataFrame]('addCanonicalTranscriptId)
    val ensemblDf = Seq(("ENSG00000228572", "X")).toDF("id", "chromosome")
    val transcriptDf =
      Seq(
        GeneAndCanonicalTranscript("ENSG00000228572",
                                   CanonicalTranscript("ENST00000431238", "X", 253743, 255091, "+")
        ),
        GeneAndCanonicalTranscript("ENSG00000228572",
                                   CanonicalTranscript("ENST00000431238", "Y", 253743, 255091, "+")
        )
      ).toDS()

    // when
    val results = Ensembl invokePrivate addTranscript(ensemblDf, transcriptDf)
    // then
    results.count() should equal(1)
  }
}
