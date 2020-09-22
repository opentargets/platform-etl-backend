package io.opentargets.etl.backend

import io.opentargets.etl.backend.drug_beta.Molecule
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object MoleculeTest {

  val simpleReferenceSchema: StructType = {
    StructType(
      StructField("id", StringType) ::
      StructField("xref", MapType(StringType, ArrayType(StringType))) :: Nil
    )
  }

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
          ArrayType(StructType(Array(StructField("xref_src", StringType),
            StructField("xref_id", StringType))),
            containsNull = false),
          nullable = false
        )
      )
    )
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }
}
class MoleculeTest extends AnyWordSpecLike with Matchers with SparkSessionSetup {

  "The Molecule class" should {

    "successfully create a map of singleton cross references" in withSparkSession { sparkSession =>
      // given
      val refColumn = "src"
      val df = MoleculeTest.getDrugbankSampleData(refColumn, sparkSession)
      val mol = new Molecule()(sparkSession)

      // when
      val results = mol.processSingletonCrossReferences(df, refColumn, "SRC")
      val crossReferences: collection.Map[String, Array[String]] = results.head.getMap[String, Array[String]](1)
      // then
      assert(crossReferences.keys.toSet.contains(refColumn.toUpperCase), "Map key should be designated source.")
      assert(crossReferences.values.size equals 1, "Singleton cross references should have a single value.")
    }

    "successfully merge two maps of references" in withSparkSession { sparkSession =>
      // given
      val x: Seq[Row] = Seq(Row("id1", Map("a" -> Array("b"))))
      val y: Seq[Row] = Seq(Row("id1", Map("c" -> Array("d"))))

      val refs1: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(x), MoleculeTest.simpleReferenceSchema)
      val refs2: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(y), MoleculeTest.simpleReferenceSchema)
      val molecule = new Molecule()(sparkSession)

      // when
      val results: DataFrame = molecule.mergeCrossReferenceMaps(refs1,refs2)
      // then
      assert(refs1.join(refs2, Seq("id"), "fullouter").select(col("id")).count() == results.count(),
        "All IDs should be returned in combined dataframe.")
      assert(results.filter(col("id") === "id1").head.getMap[String, Array[String]](1).keys.size == 2,
        "All sources from source references should be included in combined map")
    }

    "successfully create map of ChEMBL cross references" in withSparkSession { sparkSession =>
      // given
      val sources = Set("PubChem", "DailyMed")
      val df = MoleculeTest.getSampleChemblData(sparkSession)
      val mol = new Molecule()(sparkSession)

      // when
      val results: DataFrame = mol.processChemblCrossReferences(df)
      val row: Row = results.head
      val crossReferences: collection.Map[String, Array[String]] =
        row.getMap[String, Array[String]](1)
      // then
      assert(results.columns.length == 2, "Resulting data frame should have two columns.")
      assert(crossReferences.keys.toSet equals sources, s"Not all source found in source map.")
    }

  }

}
