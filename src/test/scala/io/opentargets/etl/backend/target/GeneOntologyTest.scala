package io.opentargets.etl.backend.target

import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.GeneOntologyTest.{ensemblDS, goRawDf}
import io.opentargets.etl.backend.spark.Helpers._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object GeneOntologyTest {
  def goRawDf(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read
      .options(Map("sep" -> "\\t", "comment" -> "!"))
      .csv(this.getClass.getResource("/target/goa_500.gaf").getPath)
  val ensemblDS: Seq[Ensembl] = Seq(
    Ensembl(
      "ENSG00000135392",
      "protein_coding",
      "DnaJ heat shock protein family (Hsp40) member C14",
      None,
      GenomicLocation("12", 55820960, 55830824, -1),
      "DNAJC14",
      Some(
        Array(
          IdAndSource("AAI7147", "uniprot"),
          IdAndSource("EAW96834", "uniprot"),
          IdAndSource("AAK56241", "uniprot"),
          IdAndSource("AAH80655", "uniprot"),
          IdAndSource("ABQ59051", "uniprot"),
          IdAndSource("ENSP00000504134", "ensembl_PRO")
        )
      ),
      Some(Array()),
      None
    )
  )
}

class GeneOntologyTest extends EtlSparkUnitTest {
  import sparkSession.implicits._

  "The raw gene ontology data set" should "be properly ingested with correct columns returned" in {
    // given
    val methodUnderTest = PrivateMethod[Dataset[Row]]('extractRequiredColumnsFromRawDf)
    val input = goRawDf
    // when
    val results = GeneOntology invokePrivate methodUnderTest(input)

    // then
    results.columns.length should be(6)
    results.count() should be(500)

  }
  "An Ensembl dataset" should "create an ensembl -> uniprot lookup table" in {
    // given
    val methodUnderTest = PrivateMethod[Dataset[Row]]('ensemblDfToHumanLookupTable)
    val input: Dataset[Ensembl] = ensemblDS.toDS
    // when
    val results = GeneOntology invokePrivate methodUnderTest(input, sparkSession)

    // then
    results.count() should be(6)
  }

}
