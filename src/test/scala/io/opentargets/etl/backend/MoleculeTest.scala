package io.opentargets.etl.backend

import io.opentargets.etl.backend.MoleculeTest.{getMoleculeInstance, getSampleSynonymData}
import io.opentargets.etl.backend.drug_beta.Molecule
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType, MapType, StringType, StructField, StructType}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers


object MoleculeTest {

  // Schemas for Spark testing
  val simpleReferenceSchema: StructType = {
    StructType(
      StructField("id", StringType) ::
        StructField("xref", MapType(StringType, ArrayType(StringType))) :: Nil
    )
  }
  val structAfterPreprocessing: StructType = StructType(
    StructField("id", StringType) ::
      StructField("canonical_smiles", StringType, nullable = true) ::
      StructField("type", StringType, nullable = true) ::
      StructField("chebi_par_id", LongType, nullable = true) ::
      StructField("black_box_warning", BooleanType, nullable = false) ::
      StructField("pref_name", StringType, nullable = true) ::
      StructField(
      "cross_references",
      ArrayType(
        StructType(Array(
          StructField("xref_id", StringType),
          StructField("xref_name", StringType),
          StructField("xref_src", StringType),
          StructField("xref_src_url", StringType),
          StructField("xref_url", StringType)
        ))
      )
    ) ::
      StructField("first_approval", LongType) ::
      StructField("max_clinical_trial_phase", LongType) ::
      StructField("molecule_hierarchy",
                  StructType(Array(StructField("molecule_chembl_id", StringType),
                                   StructField("parent_chembl_id", StringType)))) ::
      StructField("withdrawn_flag", BooleanType) ::
      StructField("withdrawn_year", LongType) ::
      StructField("withdrawn_reason", ArrayType(StringType)) ::
      StructField("withdrawn_country", ArrayType(StringType)) ::
      StructField("withdrawn_class", ArrayType(StringType)) ::
      StructField("syns",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("mol_synonyms", StringType),
                        StructField("synonym_type", StringType)
                      )
                    )
                  )) ::
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
              Array(StructField("xref_src", StringType), StructField("xref_id", StringType))),
            containsNull = false),
          nullable = false
        )
      )
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }

  def getSampleSynonymData(sparkSession: SparkSession): DataFrame = {
    val schema = StructType(Array(
      StructField("id", StringType),
      StructField("syns",
        ArrayType(
          StructType(
            Array(
              StructField("mol_synonyms", StringType),
              StructField("synonym_type", StringType)))))))
    val data: Seq[Row] = Seq(
      Row("id1", Seq(Row("Aches-N-Pain","trade_name"), Row("Advil", "trade_name"))),
      Row("id1", Seq(Row("Ibuprofil","UBAN"), Row("U-18573", "research_code"))),
      Row("id2", Seq(Row("Quinocort","trade_name"), Row("Terra-Cortil", "other"))),
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)

  }

  def getMoleculeInstance(sparkSession: SparkSession): Molecule = {
    new Molecule(sparkSession.emptyDataFrame, sparkSession.emptyDataFrame)(sparkSession)
  }
}
class MoleculeTest extends AnyFlatSpecLike with Matchers with PrivateMethodTester with SparkSessionSetup {

  // private methods for testing
  val processSingletonXR: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('processSingletonCrossReferences)
  val mergeXRMaps: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('mergeCrossReferenceMaps)
  val processChemblXR: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('processChemblCrossReferences)
  val processMoleculeCrossReferences: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('processMoleculeCrossReferences)
  val processMoleculeSynonyms: PrivateMethod[Dataset[Row]] = PrivateMethod[Dataset[Row]]('processMoleculeSynonyms)

  val molecule: Molecule = getMoleculeInstance(sparkSession)

  "The Molecule class" should "given a preprocessed molecule successfully prepare all cross references" in {
      // given
      val sampleMolecule: DataFrame = sparkSession.read
        .option("multiline",value = true)
        .schema(MoleculeTest.structAfterPreprocessing)
        .json(this.getClass.getResource("/sample_mol_after_preprocessing.json").getPath)
      val molecule = MoleculeTest.getMoleculeInstance(sparkSession)
      // when
      val results = molecule invokePrivate processMoleculeCrossReferences(sampleMolecule)
      val xrefMap = results.head.getMap(1)
      // then
      assertResult(4){
        xrefMap.keySet.size
      }
    }

    it should "successfully create a map of singleton cross references" in {
      // given
      val refColumn = "src"
      val df = MoleculeTest.getDrugbankSampleData(refColumn, sparkSession)
      val molecule = MoleculeTest.getMoleculeInstance(sparkSession)
      // when
      val results = molecule invokePrivate processSingletonXR(df, refColumn, "SRC")
      val crossReferences: collection.Map[String, Array[String]] =
        results.head.getMap[String, Array[String]](1)
      // then
      assert(crossReferences.keys.toSet.contains(refColumn.toUpperCase),
             "Map key should be designated source.")
      assert(crossReferences.values.size equals 1,
             "Singleton cross references should have a single value.")
    }

    it should "successfully merge two maps of references" in {
      // given
      val x: Seq[Row] = Seq(Row("id1", Map("a" -> Array("b"))))
      val y: Seq[Row] = Seq(Row("id1", Map("c" -> Array("d"))))

      val refs1: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(x),
                                                          MoleculeTest.simpleReferenceSchema)
      val refs2: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(y),
                                                          MoleculeTest.simpleReferenceSchema)
      val molecule = MoleculeTest.getMoleculeInstance(sparkSession)
      // when
      val results: DataFrame = molecule invokePrivate mergeXRMaps(refs1, refs2)
      // then
      assert(refs1.join(refs2, Seq("id"), "fullouter").select(col("id")).count() == results.count(),
             "All IDs should be returned in combined dataframe.")
      assert(
        results.filter(col("id") === "id1").head.getMap[String, Array[String]](1).keys.size == 2,
        "All sources from source references should be included in combined map"
      )
    }

    it should "successfully create map of ChEMBL cross references" in {
      // given
      val sources = Set("PubChem", "DailyMed")
      val df = MoleculeTest.getSampleChemblData(sparkSession)
      val molecule = MoleculeTest.getMoleculeInstance(sparkSession)
      // when
      val results: DataFrame = molecule invokePrivate  processChemblXR(df)
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
        Seq("a", "e"),
      )
      // when
      val map: Map[String, Seq[String]] = Molecule.createSrcToReferenceMap(input)
      // then
      assertResult(2) {
        map.keySet.size
      }
      assertResult(input.size,
                   "There should be the same number of values in the maps as there were inputs.") {
        map.values.foldLeft(0)((acc, seq) => acc + seq.size)
      }
    }

  it should "separate synonyms into trade_names and synonyms" in {
    // given
    val df = getSampleSynonymData(sparkSession)
    // when
    val results = molecule invokePrivate processMoleculeSynonyms(df)
    // then
    val expectedColumns = Set("id", "synonyms", "trade_names")
    val expectedTradeNameCount = Seq(("id1", 2), ("id2", 1))
    val expectedSynonymCount = Seq(("id1", 2), ("id2", 1))
    def testcounts(column: String, inputs: Seq[(String, Int)]): Boolean = {
      val r = for ((id, count) <- inputs) yield {
          results.filter(col("id") === id)
            .select(org.apache.spark.sql.functions.size(col(column).as("s"))).head.getAs[Int](0) == count
      }
      r.forall( v => v )
    }

    assert(results.columns.length == 3 && results.columns.forall(expectedColumns.contains), "Expected columns should be generated.")
    assert(results.count == 2, "Results should be grouped by ID")
    assert(testcounts("trade_names", expectedTradeNameCount), "The correct number of trade names are grouped")
    assert(testcounts("synonyms", expectedSynonymCount), "The correct number of synonyms are grouped.")

  }
}
