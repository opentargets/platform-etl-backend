package io.opentargets.etl.backend.DrugBeta

import io.opentargets.etl.backend.Configuration.InputExtension
import io.opentargets.etl.backend.DrugBeta.SynonymExtensionTest.{Molecule, Synonym, SynonymArr, SynonymBadIdField, SynonymBadSynonymField}
import io.opentargets.etl.backend.SparkSessionSetup
import io.opentargets.etl.backend.drug_beta.DrugExtensions
import io.opentargets.etl.backend.spark.Helpers
import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers

class DrugExtensionsTest extends AnyFlatSpecLike with Matchers {

  "An extension file" should "must be a json file" in {
    assertThrows[IllegalArgumentException] {
      val extensionInvalid = InputExtension("synonym", "gs://foo/bar/baz.csv")
    }
  }
}

object SynonymExtensionTest {
  // These case classes are simulating what would be read in from a Json file of inputs
  case class SynonymBadSynonymField(id: String, names: String)
  case class SynonymBadIdField(ids: String, synonyms: String)
  case class Synonym(id: String, synonyms: String)
  case class SynonymArr(id: String, synonyms: Array[String])
  // Minimum molecule DF
  case class Molecule(id: String, drugbank_id: String, tradeNames: Array[String], synonyms: Array[String])
}
class SynonymExtensionTest extends AnyFlatSpecLike with PrivateMethodTester with SparkSessionSetup with Matchers {

  val stardardiseSynonyms: PrivateMethod[Dataset[Row]] = {
    PrivateMethod[Dataset[Row]]('standardiseSynonymsDf)
  }
  val addSynonymsToMolecule: PrivateMethod[Dataset[Row]] = {
    PrivateMethod[Dataset[Row]]('addSynonymsToMolecule)
  }

  "Synonym extension input files" must "have at least two columns, 'id' and 'synonym'" in {
    // given
    import sparkSession.implicits._

    // when
    assertThrows[AssertionError]{
      val inputBad1: DataFrame = Seq(SynonymBadSynonymField("D01", "foo"), SynonymBadSynonymField("D01", "bar")).toDF
      val result = DrugExtensions invokePrivate stardardiseSynonyms(inputBad1)
    }
    assertThrows[AssertionError]{
      val inputBad2 =  Seq(SynonymBadIdField("D01", "foo"), SynonymBadIdField("D01", "bar")).toDF
      val result = DrugExtensions invokePrivate stardardiseSynonyms(inputBad2)
    }
  }

  "Synonym extension input files with String field for synonym" should "be grouped by id" in {
    // given
    import sparkSession.implicits._
    val inputDf: DataFrame = Seq(Synonym("D01", "foo"), Synonym("D01", "bar")).toDF
    // when
    val results = DrugExtensions invokePrivate stardardiseSynonyms(inputDf)
    // then
    assert(results.schema("synonyms").dataType.isInstanceOf[ArrayType],
      s"Expected array but was ${results.schema("synonyms").dataType.typeName}")
  }

  "Adding synonyms to molecule" should "not add duplicate synonyms" in {
    import sparkSession.implicits._
    // given
    val id = "CHEMBL1"
    val syn = "hydroxychloroquine"
    val molDf = Seq(Molecule(id, "", Array(), Array(syn))).toDF
    val synDf = Seq(SynonymArr(id, Array(syn))).toDF
    // when
    val results = DrugExtensions invokePrivate addSynonymsToMolecule(molDf, synDf)
    val synCount: Integer = results.withColumn("count", org.apache.spark.sql.functions.size(col("synonyms")))
      .head
      .getAs[Integer]("count")
    // then
    assert(synCount equals 1)
  }

  it should "not add synonyms that are already in tradeNames" in {
    import sparkSession.implicits._
    // given
    val id = "CHEMBL1"
    val syn = "hydroxychloroquine"
    val molDf = Seq(Molecule(id, "", Array(syn), Array())).toDF
    val synDf = Seq(SynonymArr(id, Array(syn))).toDF
    // when
    val results = DrugExtensions invokePrivate addSynonymsToMolecule(molDf, synDf)
    val synCount: Integer = results.withColumn("count", org.apache.spark.sql.functions.size(col("synonyms")))
      .head
      .getAs[Integer]("count")
    // then
    assert(synCount equals 0)

  }

  it should "add synonyms that are not already tradeNames or synonyms" in {
    import sparkSession.implicits._
    // given
    val id = "CHEMBL1"
    val newSynonyms = Array("s1", "s2", "s3", "s4")
    val molDf = Seq(Molecule(id, "", Array("s1"), Array("s2"))).toDF
    val synDf = Seq(SynonymArr(id, newSynonyms)).toDF
    // when
    val results = DrugExtensions invokePrivate addSynonymsToMolecule(molDf, synDf)
    val synCount: Integer = results.withColumn("count", org.apache.spark.sql.functions.size(col("synonyms")))
      .head
      .getAs[Integer]("count")
    // then
    assert(synCount equals 3)
  }
}
