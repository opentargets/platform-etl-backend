package io.opentargets.etl.backend.target

import better.files.File
import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.target.UniprotTest.DbTest
import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object UniprotTest {
  val uniprotDataPath: String = this.getClass.getResource("/uniprot/sample_10.txt").getPath
  lazy val uniprotDataStream: Iterator[String] = File(uniprotDataPath).lineIterator

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
}
