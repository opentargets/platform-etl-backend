package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.GenCodeTest.genCodeRawDf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object GenCodeTest {
  def genCodeRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(this.getClass.getResource("/target/genCode50.jsonl").getPath)
}
class GenCodeTest extends EtlSparkUnitTest {

  // given
  val dfRaw: DataFrame = genCodeRawDf

  // when
  val genCodeDf: Dataset[GeneAndCanonicalTranscript] = GeneCode(dfRaw).cache

  "Canonical transcript" should "be extracted from GenCode data" in {
    val rowCount = genCodeDf.count()
    import sparkSession.implicits._
    genCodeDf.select(col("id")).distinct.count() should be(rowCount)
    genCodeDf.map(_.canonicalTranscript.id.distinct).count() should be(rowCount)
    genCodeDf
      .filter(col("canonicalTranscript.start") > col("canonicalTranscript.end"))
      .count() should be(0)
    genCodeDf
      .filter(!col("canonicalTranscript.strand").isInCollection(Seq("+", "-")))
      .count() should be(0)
  }
}
