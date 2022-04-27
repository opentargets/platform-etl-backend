package io.opentargets.etl.backend.Drug

import io.opentargets.etl.backend.Drug.CrossReferencesExtensionTest.SimpleCrossReference
import io.opentargets.etl.backend.Drug.SynonymExtensionTest._
import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.drug.DrugExtensions
import org.apache.spark.sql.functions.{
  col,
  element_at,
  explode,
  flatten,
  map_keys,
  map_values,
  size => sparkSize
}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object CrossReferencesExtensionTest {
  case class SimpleCrossReference(
      id: String,
      source: String,
      reference: String
  ) // an extension file has at least one ref field
}

class CrossReferencesExtensionTest extends EtlSparkUnitTest {
  val addCrossReferenceToMolecules: PrivateMethod[Dataset[Row]] = {
    PrivateMethod[Dataset[Row]]('addCrossReferenceToMolecule)
  }

  // given a cross reference and the field doesn't already exist, it's added to the cross reference field
  "A cross reference extension file with a new reference field" should "be added to the existing references" in {
    import sparkSession.implicits._
    // given
    val id = "id"
    val molDf = Seq(Molecule(id, "", Array(), Array())).toDF
    val xRefDf = Seq(SimpleCrossReference(id, "src1", "ref1")).toDF
    // when
    val results = DrugExtensions invokePrivate addCrossReferenceToMolecules(molDf, xRefDf)
    // then
    assertResult(1)(results.select(map_keys(col("crossReferences"))).count())
    assertResult("ref1")(
      results
        .select(explode(element_at(col("crossReferences"), "src1")))
        .first()
        .getString(0)
    )
  }
  // given a cross reference and the field does exist, it is added to the array
  "A cross reference extension file with an existing reference field" should "be added to the existing references" in {
    import sparkSession.implicits._
    // given
    val id = "id"
    val molDf = Seq(Molecule(id, "", Array(), Array(), Map("src" -> Array("existingRef")))).toDF
    val xRefDf = Seq(SimpleCrossReference(id, "src", "ref1")).toDF
    // when
    val results = DrugExtensions invokePrivate addCrossReferenceToMolecules(molDf, xRefDf)
    // then
    assertResult(1)(results.select(map_keys(col("crossReferences"))).count())
    assertResult(2)(results.select(explode(flatten(map_values(col("crossReferences"))))).count())
  }
  // given multiple new fields they are all added
  "A cross reference extension file with multiple new fields" should "all be added to the existing references" in {
    import sparkSession.implicits._
    // given
    val id = "id"
    val molDf = Seq(Molecule(id, "", Array(), Array(), Map("src" -> Array("existingRef")))).toDF
    val xRefDf = Seq(
      SimpleCrossReference(id, "src", "ref1"),
      SimpleCrossReference(id, "src1", "ref2"),
      SimpleCrossReference(id, "src2", "ref3")
    ).toDF
    // when
    val results = DrugExtensions invokePrivate addCrossReferenceToMolecules(molDf, xRefDf)
    // then
    assertResult(3, "All keys should be in the returned map")(
      results.select(sparkSize(map_keys(col("crossReferences")))).head.getInt(0)
    )
    assertResult(4, "All new references should be in the returned map")(
      results.select(explode(flatten(map_values(col("crossReferences"))))).count()
    )
    assertResult(2, "New reference should be added to existing key")(
      results.select(explode(element_at(col("crossReferences"), "src"))).count()
    )
  }

  "A cross reference extension file" should "only update targeted fields" in {
    import sparkSession.implicits._
    // given
    val id = "id"
    val refMap = Map(
      "existingSrc1" -> Array("es1", "es2"),
      "existingSrc2" -> Array("es3", "es4")
    )
    val molDf = Seq(Molecule(id, "", Array(), Array(), refMap)).toDF
    val xRefDf = Seq(
      SimpleCrossReference(id, "src", "ref1"),
      SimpleCrossReference(id, "src", "ref2"),
      SimpleCrossReference(id, "src", "ref3"),
      SimpleCrossReference(id, "existingSrc1", "ref3")
    ).toDF
    // when
    val results = DrugExtensions invokePrivate addCrossReferenceToMolecules(molDf, xRefDf)
    // then
    assertResult(2, "Existing reference should not be changed")(
      results.select(explode(element_at(col("crossReferences"), "existingSrc2"))).count()
    )
  }

}

object SynonymExtensionTest {
  // These case classes are simulating what would be read in from a Json file of inputs
  case class SynonymBadSynonymField(id: String, names: String)
  case class SynonymBadIdField(ids: String, synonyms: String)
  case class Synonym(id: String, synonyms: String)
  case class SynonymArr(id: String, synonyms: Array[String])
  case class SynonymLong(
      id: String,
      synonyms: String,
      foo: String,
      bar: String
  ) // extra columns should not effect result
  // Minimum molecule DF
  case class Molecule(
      id: String,
      drugbank_id: String,
      tradeNames: Array[String],
      synonyms: Array[String],
      crossReferences: Map[String, Array[String]] = Map.empty[String, Array[String]]
  )
}
class SynonymExtensionTest extends EtlSparkUnitTest {

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
    assertThrows[AssertionError] {
      val inputBad1: DataFrame =
        Seq(SynonymBadSynonymField("D01", "foo"), SynonymBadSynonymField("D01", "bar")).toDF
      val result = DrugExtensions invokePrivate stardardiseSynonyms(inputBad1)
    }
    assertThrows[AssertionError] {
      val inputBad2 = Seq(SynonymBadIdField("D01", "foo"), SynonymBadIdField("D01", "bar")).toDF
      val result = DrugExtensions invokePrivate stardardiseSynonyms(inputBad2)
    }
  }

  "Data in input files in unknown columns" should "be discarded" in {
    import sparkSession.implicits._
    // given
    val id = "id"
    val syn = "syn"
    val molDf = Seq(Molecule(id, "", Array(), Array(syn))).toDF
    val synDf =
      DrugExtensions invokePrivate stardardiseSynonyms(Seq(SynonymLong(id, syn, "foo", "bar")).toDF)
    // when
    val results = DrugExtensions invokePrivate addSynonymsToMolecule(molDf, synDf)
    // then
    assert(results.columns.length equals molDf.columns.length)
  }

  "Synonym extension input files with String field for synonym" should "be grouped by id" in {
    // given
    import sparkSession.implicits._
    val inputDf: DataFrame = Seq(Synonym("D01", "foo"), Synonym("D01", "bar")).toDF
    // when
    val results = DrugExtensions invokePrivate stardardiseSynonyms(inputDf)
    // then
    assert(
      results.schema("synonyms").dataType.isInstanceOf[ArrayType],
      s"Expected array but was ${results.schema("synonyms").dataType.typeName}"
    )
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
    val synCount: Integer = results
      .withColumn("count", org.apache.spark.sql.functions.size(col("synonyms")))
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
    val synCount: Integer = results
      .withColumn("count", org.apache.spark.sql.functions.size(col("synonyms")))
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
    val synCount: Integer = results
      .withColumn("count", org.apache.spark.sql.functions.size(col("synonyms")))
      .head
      .getAs[Integer]("count")
    // then
    assert(synCount equals 3)
  }
}
