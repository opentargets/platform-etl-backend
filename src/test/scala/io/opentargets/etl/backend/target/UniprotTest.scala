package io.opentargets.etl.backend.target

import better.files.File
import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.UniprotTest.{DbTest, uniprotData, uniprotSslDataPath}
import io.opentargets.etl.preprocess.uniprot.{UniprotConverter, UniprotEntry}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.matchers.should.Matchers._

object UniprotTest {
  val uniprotDataPath: String = this.getClass.getResource("/uniprot/sample_10.txt").getPath
  val uniprotSslDataPath: String =
    this.getClass.getResource("/uniprot/subcellularLocationUniprot.tsv").getPath
  lazy val uniprotDataStream: Iterator[String] = File(uniprotDataPath).lineIterator
  lazy val uniprotData: Seq[UniprotEntry] = {
    UniprotConverter.fromFlatFile(uniprotDataStream)
  }

  case class DbTest(uniprotId: String, dbXrefs: Seq[String])

}

class UniprotTest extends EtlSparkUnitTest {

  "dbXrefs" should "be correctly transformed into id and source elements" in {
    import sparkSession.implicits._
    // given
    val databaseTestInputs =
      Seq(DbTest("1", Seq("PDB 5M7R", "ChEMBL CHEMBL5921", "DrugBank DB00428", "PDB 2YDQ")))
    val input: Dataset[Row] = databaseTestInputs.toDF
    val methodUnderTest = PrivateMethod[Dataset[Row]]('handleDbRefs)
    // when
    val results = Uniprot invokePrivate methodUnderTest(input)

    // then
    results
      .filter(col("uniprotId") === "1")
      .select(sql.functions.size(col("dbXrefs")))
      .head()
      .get(0) should be(4)
  }
  "Uniprot proteinIds" should "be labelled as obsolete" in {
    import sparkSession.implicits._
    // given
    val input: Dataset[Row] = uniprotData.toDF
    val sslDf: DataFrame =
      sparkSession.read.option("header", "true").option("sep", "\\t").csv(uniprotSslDataPath)
    // when
    val results = Uniprot(input, sslDf)

    // then
    results
      .select(explode(col("proteinIds")).as("pid"))
      .select("pid.source")
      .distinct
      .head
      .get(0) should be("uniprot_obsolete")
  }

  "Uniprot subcellular location ontology terms" should "be added to Uniprot data" in {
    import sparkSession.implicits._
    // given
    val methodUnderTest = PrivateMethod[Dataset[Row]]('mapLocationsToSsl)
    val uniprotDf: DataFrame = Seq(
      ("CDKL5", Array("Cytoplasm, cytoskeleton, microtubule organizing center, centrosome")),
      ("P48634", Array("Cytoplasm", "Nucleus")),
      (
        "MAOB",
        Array(
          "Mitochondrion outer membrane; Single-pass type IV membrane protein; Cytoplasmic side"
        )
      ),
      ("P38398", Array("[Isoform 3]: Cytoplasm", "Nucleus"))
    ).toDF("uniprotId", "locations")
    val uniprotSsl: DataFrame = Seq(
      ("SL1", "Cytoplasm", "Component")
    ).toDF("Subcellular location ID", "Alias", "Category")

    // when
    val results = Uniprot invokePrivate methodUnderTest(uniprotSsl, uniprotDf)

    def getLocationFor(uniprotId: String): DataFrame = results
      .filter(col("uniprotId") === uniprotId)
      .select(explode(col("subcellularLocations")) as "locations")
    // then
    // no uniprot entries should be lost
    results.count() should be(uniprotDf.count())
    // no locations should be lost
    getLocationFor("P48634")
      .count() should be(2)
    // entries with ';' should only include first term
    getLocationFor("MAOB")
      .filter(col("locations.location").equalTo("Mitochondrion outer membrane"))
      .count() should be(1)
    // isoform entries should be mapped to SL ontology but still listed as [iso...
    getLocationFor("P38398")
      .filter(col("locations.location").equalTo("[Isoform 3]: Cytoplasm"))
      .count() should be(1)
  }

}
