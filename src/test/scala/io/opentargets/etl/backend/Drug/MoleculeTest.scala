package io.opentargets.etl.backend.Drug

import io.opentargets.etl.backend.Drug.MoleculeTest.{
  XRef,
  getSampleHierarchyData,
  getSampleSynonymData,
  getSampleWithdrawnNoticeData
}
import io.opentargets.etl.backend.EtlSparkUnitTest
import io.opentargets.etl.backend.drug.Molecule
import org.apache.spark.sql.functions.{array_contains, col, map_keys}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object MoleculeTest {

  // Schemas for Spark testing
  val structAfterPreprocessing: StructType = StructType(
    StructField("id", StringType) ::
      StructField("canonical_smiles", StringType, nullable = true) ::
      StructField("drugType", StringType, nullable = true) ::
      StructField("chebi_par_id", LongType, nullable = true) ::
      StructField("blackBoxWarning", BooleanType, nullable = false) ::
      StructField("name", StringType, nullable = true) ::
      StructField(
        "cross_references",
        ArrayType(
          StructType(
            Array(
              StructField("xref_id", StringType),
              StructField("xref_name", StringType),
              StructField("xref_src", StringType),
              StructField("xref_src_url", StringType),
              StructField("xref_url", StringType)
            )
          )
        )
      ) ::
      StructField("yearOfFirstApproval", LongType) ::
      StructField("maximumClinicalTrialPhase", LongType) ::
      StructField(
        "molecule_hierarchy",
        StructType(
          Array(
            StructField("molecule_chembl_id", StringType),
            StructField("parent_chembl_id", StringType)
          )
        )
      ) ::
      StructField("hasBeenWithdrawn", BooleanType) ::
      StructField("withdrawn_year", LongType) ::
      StructField("withdrawn_reason", ArrayType(StringType)) ::
      StructField("withdrawn_country", ArrayType(StringType)) ::
      StructField("withdrawn_class", ArrayType(StringType)) ::
      StructField(
        "syns",
        ArrayType(
          StructType(
            Array(
              StructField("mol_synonyms", StringType),
              StructField("synonym_type", StringType)
            )
          )
        )
      ) ::
      StructField("drugbank_id", StringType) ::
      Nil
  )

  def getDrugbankSampleData(refColumn: String, sparkSession: SparkSession): DataFrame = {
    val schema = StructType(
      StructField("id", StringType) ::
        StructField(refColumn, StringType) :: Nil
    )
    val refs = Seq(
      Row("CHEMB614", "DB00339"),
      Row("CHEMB1269257", "DB12309"),
      Row("CHEMB295698", "DB05667"),
      Row("CHEMB1427", "DB04076"),
      Row("CHEMB1289601", "DB09078")
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(refs), schema)
  }

  def getSampleChemblData(sparkSession: SparkSession): DataFrame = {
    val chemblIdWithPubChemAndDailyMedSources = "CHEMBL1201042"

    val refs = Seq(
      Row("PubChem", "11112988"),
      Row("PubChem", "144205149"),
      Row("PubChem", "170464730"),
      Row("DailyMed", "etidronate disodium")
    )
    val data = Seq(
      Row(chemblIdWithPubChemAndDailyMedSources, refs)
    )
    val schema = StructType(
      Array(
        StructField("id", StringType, nullable = false),
        StructField(
          "cross_references",
          ArrayType(
            StructType(
              Array(StructField("xref_src", StringType), StructField("xref_id", StringType))
            ),
            containsNull = false
          ),
          nullable = false
        )
      )
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }

  def getSampleHierarchyData(sparkSession: SparkSession): DataFrame = {
    val schema = StructType(
      Array(
        StructField("id", StringType),
        StructField("parentId", StringType)
      )
    )

    val data: Seq[Row] = Seq(
      Row("a", "a"),
      Row("b", "a"),
      Row("c", "a"),
      Row("d", "c")
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }

  def getSampleSynonymData(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val schema = StructType(
      Array(
        StructField("id", StringType),
        StructField("drugbank_id", StringType),
        StructField(
          "syns",
          ArrayType(
            StructType(
              Array(
                StructField("mol_synonyms", StringType),
                StructField("synonym_type", StringType)
              )
            )
          )
        )
      )
    )
    val data: Seq[Row] = Seq(
      Row("id1", "DB01", Seq(Row("Aches-N-Pain", "trade_name"), Row("Advil", "trade_name"))),
      Row("id1", "DB01", Seq(Row("Ibuprofil", "UBAN"), Row("U-18573", "research_code"))),
      Row("id2", "DB02", Seq(Row("Quinocort", "trade_name"), Row("Terra-Cortil", "other")))
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }

  def getSampleWithdrawnNoticeData(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val schema = StructType(
      Array(
        StructField("id", StringType),
        StructField("hasBeenWithdrawn", BooleanType),
        StructField("withdrawn_class", ArrayType(StringType)),
        StructField("withdrawn_country", ArrayType(StringType)),
        StructField("withdrawn_year", LongType)
      )
    )

    val data: Seq[Row] = Seq(
      Row("id1", true, Seq(), Seq("United States"), 1965L),
      Row("id2", false, null, null, null),
      Row("id3", false, null, null, null)
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }

  case class DrugbankSynonym(drugbank_id: String, db_synonyms: Seq[String])
  case class XRef(id: String, xref: Map[String, Array[String]])
}
class MoleculeTest extends EtlSparkUnitTest {

  // private methods for testing
  val processSingletonXR: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('processSingletonCrossReferences)
  val mergeXRMaps: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('mergeCrossReferenceMaps)
  val processChemblXR: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('processChemblCrossReferences)
  val processMoleculeCrossReferences: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('processMoleculeCrossReferences)
  val processMoleculeSynonyms: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('processMoleculeSynonyms)
  val processMoleculeHierarchy: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('processMoleculeHierarchy)
  val processWithdrawnNotice: PrivateMethod[Dataset[Row]] =
    PrivateMethod[Dataset[Row]]('processWithdrawnNotices)

  "The Molecule class" should "given a preprocessed molecule successfully prepare all cross references" in {
    // given
    val sampleMolecule: DataFrame = sparkSession.read
      .option("multiline", value = true)
      .schema(MoleculeTest.structAfterPreprocessing)
      .json(this.getClass.getResource("/sample_mol_after_preprocessing.json").getPath)
    // when
    val results = Molecule invokePrivate processMoleculeCrossReferences(sampleMolecule)
    val xrefMap = results.head.getMap(1)
    // then
    assertResult(4) {
      xrefMap.keySet.size
    }
  }

  it should "successfully create a map of singleton cross references" in {
    // given
    val refColumn = "src"
    val df = MoleculeTest.getDrugbankSampleData(refColumn, sparkSession)
    // when
    val results = Molecule invokePrivate processSingletonXR(df, refColumn, "SRC")
    val crossReferences: collection.Map[String, Array[String]] =
      results.head.getMap[String, Array[String]](1)
    // then
    assert(
      crossReferences.keys.toSet.contains(refColumn.toUpperCase),
      "Map key should be designated source."
    )
    assert(
      crossReferences.values.size equals 1,
      "Singleton cross references should have a single value."
    )
  }

  it should "successfully merge two maps of references" in {
    // given
    import sparkSession.implicits._
    val refs1 = Seq(XRef("id1", Map("a" -> Array("b")))).toDF
    val refs2 = Seq(XRef("id1", Map("c" -> Array("d")))).toDF

    // when
    val results: DataFrame = Molecule invokePrivate mergeXRMaps(refs1, refs2)
    // then
    assert(
      refs1.join(refs2, Seq("id"), "fullouter").select(col("id")).count() == results.count(),
      "All IDs should be returned in combined dataframe."
    )
    assert(
      results
        .select(org.apache.spark.sql.functions.size(map_keys(col("xref"))))
        .as("s")
        .head()
        .getInt(0) == 2,
      "All sources from source references should be included in combined map"
    )
  }
  it should "successfully merge two maps of references when one column is null" in {
    // given
    import sparkSession.implicits._
    val refs1 = Seq(XRef("id1", Map("a" -> Array("b")))).toDF
    val refs2 = Seq(XRef("id1", null), XRef("id2", Map("a" -> Array("b")))).toDF

    // when
    val results: DataFrame = Molecule invokePrivate mergeXRMaps(refs1, refs2)
    // then
    assert(
      refs1.join(refs2, Seq("id"), "fullouter").select(col("id")).count() == results.count(),
      "All IDs should be returned in combined dataframe."
    )
    assert(
      results.filter(array_contains(map_keys(col("xref")), "a")).count() == 2,
      "All sources from source references should be included in combined map"
    )
  }
  it should "successfully create map of ChEMBL cross references" in {
    // given
    val sources = Set("PubChem", "DailyMed")
    val df = MoleculeTest.getSampleChemblData(sparkSession)
    // when
    val results: DataFrame = Molecule invokePrivate processChemblXR(df)
    val row: Row = results.head
    val crossReferences: collection.Map[String, Array[String]] =
      row.getMap[String, Array[String]](1)
    // then
    assert(results.columns.length == 2, "Resulting data frame should have two columns.")
    assert(crossReferences.keys.toSet equals sources, s"Not all source found in source map.")
  }

  it should "create map of sources from pairs represented as sequences" in {
    // given
    val input = Seq(
      Seq("a", "b"),
      Seq("a", "c"),
      Seq("b", "c"),
      Seq("b", "d"),
      Seq("a", "e")
    )
    // when
    val map: Map[String, Seq[String]] = Molecule.createSrcToReferenceMap(input)
    // then
    assertResult(2) {
      map.keySet.size
    }
    assertResult(
      input.size,
      "There should be the same number of values in the maps as there were inputs."
    ) {
      map.values.foldLeft(0)((acc, seq) => acc + seq.size)
    }
  }

  it should "correctly create a parent -> child hierarchy" in {
    // given
    val df = getSampleHierarchyData(sparkSession)
    // when
    val results = Molecule invokePrivate processMoleculeHierarchy(df)
    // then
    val expectedColumns = Set("id", "childChemblIds")
    assert(results.count == 2, "Two inputs had children so two rows should be returned.")
    assert(
      results.columns.length == expectedColumns.size && results.columns.forall(
        expectedColumns.contains
      ),
      "All expected columns should be present"
    )
    assert(
      results.filter(col("id") === "a").head.getList[String](1).size == 2,
      "Id 'a' should have two children."
    )
  }

  it should "separate synonyms into tradeNames and synonyms" in {
    // given
    val df = getSampleSynonymData(sparkSession)
    // when
    val results = Molecule invokePrivate processMoleculeSynonyms(df)
    // then
    val expectedColumns = Set("id", "synonyms", "tradeNames")
    val expectedTradeNameCount = Seq(("id1", 2), ("id2", 1))
    val expectedSynonymCount = Seq(("id1", 2), ("id2", 1))
    def testcounts(column: String, inputs: Seq[(String, Int)]): Boolean = {
      val r = for ((id, count) <- inputs) yield {
        results
          .filter(col("id") === id)
          .select(org.apache.spark.sql.functions.size(col(column).as("s")))
          .head
          .getAs[Int](0) == count
      }
      r.forall(v => v)
    }

    assert(
      results.columns.length == 3 && results.columns.forall(expectedColumns.contains),
      "Expected columns should be generated."
    )
    assert(results.count == 2, "Results should be grouped by ID")
    assert(
      testcounts("tradeNames", expectedTradeNameCount),
      "The correct number of trade names are grouped"
    )
    assert(
      testcounts("synonyms", expectedSynonymCount),
      "The correct number of synonyms are grouped."
    )

  }

  it should "only include a withdrawn notice struct when the drug is withdrawn" in {
    // given
    val df = getSampleWithdrawnNoticeData(sparkSession)
    // when
    val results = Molecule invokePrivate processWithdrawnNotice(df)
    // then
    assertResult(2L)(results.filter(col("withdrawnNotice").isNull).count())
  }
}
