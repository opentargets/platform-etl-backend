package io.opentargets.etl.backend.evidence



import io.opentargets.etl.backend.{Configuration, ETLSessionContext}
import io.opentargets.etl.backend.Configuration.OTConfig
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvidenceDatingTest extends AnyFlatSpec with Matchers {

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("EvidenceTest")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()

  // Mock ETLSessionContext for testing
  implicit val mockContext: ETLSessionContext = ETLSessionContext(
    configuration = null.asInstanceOf[OTConfig], // We'll mock this as needed for specific tests
    sparkSession = sparkSession
  )

  // Shared test data available to all tests
  val evidenceSchema = StructType(Array(
    StructField("id", StringType, nullable = false),
    StructField("releaseDate", StringType, nullable = true),
    StructField("literature", ArrayType(StringType), nullable = true)
  ))

    val testEvidenceData = sparkSession.createDataFrame(
        sparkSession.sparkContext.parallelize(
            Seq(
                Row("e1", null, Array.empty[String]), // No dates, empty array instead of null
                Row("e2", "2021-02-03", Array.empty[String]), // Release date is given, empty array
                Row("e3", "2021-02-03", Array("123", "PMC456")), // Both release date and literature is given
                Row("e4", null, Array("123", "PMC456")), // Only literature is given
                Row("e5", null, Array("PMC456")) // Only literature but only one source.
            )
        ),
        evidenceSchema
    )

    val literatureMapSchema = StructType(
            Array(
                StructField("source", StringType, nullable = false),
                StructField("firstPublicationDate", StringType, nullable = true),
                StructField("pmid", StringType, nullable = true),
                StructField("id", StringType, nullable = true),
                StructField("pmcid", StringType, nullable = true)
            )
        )

    val testPublicationData = sparkSession.createDataFrame(
        sparkSession.sparkContext.parallelize(
            Seq(
                Row("MED", "2021-06-15", "123", "123", "PMC9936"),
                Row("MED", "2021-08-15", null, "PMC456", "PMC456"),
                Row("AGR", "2021-07-30", "AGR001", "AGR001", null)
            )
        ),
        literatureMapSchema
    )

  "resolvePublicationDates" should "return dataframe" in {
    // Apply the function using shared test data
    val result = Evidence.resolvePublicationDates(testEvidenceData, testPublicationData)

    // Compile-time type assertion
    implicitly[result.type <:< DataFrame]
    
    // Test that DataFrame is created successfully
    result.count() should be(5)
    result.columns should contain("evidenceDate")
    
    // // Assert return type and schema
    // result shouldBe a[DataFrame]
    
    // // Assert specific columns exist
    // result.columns should contain allOf("id", "releaseDate", "literature", "evidenceDate")
    
    // // Assert column types
    // val schema = result.schema
    // schema("id").dataType should be(StringType)
    // schema("releaseDate").dataType should be(StringType)
    // schema("literature").dataType should be(ArrayType(StringType, true))
    // schema("evidenceDate").dataType should be(DateType)
    
    // // Assert nullable properties
    // schema("id").nullable should be(false)
    // schema("releaseDate").nullable should be(true)
    // schema("evidenceDate").nullable should be(true)
  }

//   "resolvePublicationDates" should "have correct function signature and return type" in {
//     // Apply the function using shared test data  
//     val result = Evidence.resolvePublicationDates(testEvidenceData, testPublicationData)
    
//     // Type-level assertions
//     val method = Evidence.getClass.getMethods.find(_.getName == "resolvePublicationDates").get
//     method.getReturnType should be(classOf[DataFrame])
    
//     // Parameter type assertions
//     val paramTypes = method.getParameterTypes
//     paramTypes should have length 2
//     paramTypes(0) should be(classOf[DataFrame])
//     paramTypes(1) should be(classOf[DataFrame])
//   }

//   "resolvePublicationDates" should "correctly resolve publication dates from literature identifiers" in {
//     // Apply the function using shared test data
//     val result = Evidence.resolvePublicationDates(testEvidenceData, testPublicationData)

//     // Collect results for assertions
//     val resultData = result.select("id", "evidenceDate", "publicationDate").collect()

//     // Assertions
//     resultData should have length 5

//     // e1: No dates - should fall back to releaseDate (null), evidenceDate should be null
//     val evidence1 = resultData.find(_.getString(0) == "e1").get
//     Option(evidence1.getDate(1)) should be(None) // evidenceDate should be null (no releaseDate or publicationDate)
//     Option(evidence1.getDate(2)) should be(None) // publicationDate should be null

//     // e2: Only release date - should use releaseDate
//     val evidence2 = resultData.find(_.getString(0) == "e2").get
//     evidence2.getDate(1) should not be null // evidenceDate should be releaseDate
//     Option(evidence2.getDate(2)) should be(None) // publicationDate should be null

//     // e3: Both release date and literature - should prioritize publication date
//     val evidence3 = resultData.find(_.getString(0) == "e3").get
//     evidence3.getDate(1) should not be null // evidenceDate should be publication date
//     evidence3.getDate(2) should not be null // publicationDate should be resolved

//     // e4: Only literature - should use publication date
//     val evidence4 = resultData.find(_.getString(0) == "e4").get
//     evidence4.getDate(1) should not be null // evidenceDate should be publication date
//     evidence4.getDate(2) should not be null // publicationDate should be resolved

//     // e5: Only one literature source - should use publication date
//     val evidence5 = resultData.find(_.getString(0) == "e5").get
//     evidence5.getDate(1) should not be null // evidenceDate should be publication date
//     evidence5.getDate(2) should not be null // publicationDate should be resolved
//   }

//   "resolvePublicationDates" should "prioritize earliest publication date when multiple publications exist" in {
//     // Create test evidence with multiple literature references
//     val evidenceData = Seq(
//       ("evidence1", "2023-01-01", Seq("PMID123", "PMID456"))
//     ).toDF("id", "releaseDate", "literature")

//     // Create publication data with different dates for same evidence
//     val publicationData = Seq(
//       ("MED", "2022-08-15", "PMID123", "PMID123", ""), // Later date
//       ("MED", "2022-06-10", "PMID456", "PMID456", "")  // Earlier date - should be selected
//     ).toDF("source", "firstPublicationDate", "pmid", "id", "pmcid")
//       .withColumn("pmcid", when(col("pmcid") === "", lit(null).cast(StringType)).otherwise(col("pmcid")))

//     val result = Evidence.resolvePublicationDates(evidenceData, publicationData)
//     val resultRow = result.select("id", "publicationDate").collect().head

//     // Should select the earlier publication date (2022-06-10)
//     resultRow.getDate(1).toString should be("2022-06-10")
//   }

//   "resolvePublicationDates" should "filter sources correctly (MED, PPR, AGR only)" in {
//     val evidenceData = Seq(
//       ("evidence1", "2023-01-01", Seq("PMID123"))
//     ).toDF("id", "releaseDate", "literature")

//     val publicationData = Seq(
//       ("MED", "2022-06-15", "PMID123", "PMID123", ""), // Should be included
//       ("DOI", "2022-06-15", "PMID123", "PMID123", ""), // Should be filtered out
//       ("OTHER", "2022-06-15", "PMID123", "PMID123", "") // Should be filtered out
//     ).toDF("source", "firstPublicationDate", "pmid", "id", "pmcid")
//       .withColumn("pmcid", when(col("pmcid") === "", lit(null).cast(StringType)).otherwise(col("pmcid")))

//     val result = Evidence.resolvePublicationDates(evidenceData, publicationData)
//     val resultRow = result.select("id", "publicationDate").collect().head

//     // Should only match MED source
//     resultRow.getDate(1) should not be null
//   }

//   "resolvePublicationDates" should "handle case-insensitive publication ID matching" in {
//     val evidenceData = Seq(
//       ("evidence1", "2023-01-01", Seq("pmid123", " PMID456 ")) // lowercase and with spaces
//     ).toDF("id", "releaseDate", "literature")

//     val publicationData = Seq(
//       ("MED", "2022-06-15", "PMID123", "PMID123", ""),
//       ("MED", "2022-07-15", "PMID456", "PMID456", "")
//     ).toDF("source", "firstPublicationDate", "pmid", "id", "pmcid")
//       .withColumn("pmcid", when(col("pmcid") === "", lit(null).cast(StringType)).otherwise(col("pmcid")))

//     val result = Evidence.resolvePublicationDates(evidenceData, publicationData)
//     val resultRow = result.select("id", "publicationDate").collect().head

//     // Should successfully match despite case differences and spaces
//     resultRow.getDate(1) should not be null
//   }

//   "resolvePublicationDates" should "properly cast dates to DateType" in {
//     val evidenceData = Seq(
//       ("evidence1", "2023-01-01", Seq("PMID123"))
//     ).toDF("id", "releaseDate", "literature")

//     val publicationData = Seq(
//       ("MED", "2022-06-15", "PMID123", "PMID123", "")
//     ).toDF("source", "firstPublicationDate", "pmid", "id", "pmcid")
//       .withColumn("pmcid", when(col("pmcid") === "", lit(null).cast(StringType)).otherwise(col("pmcid")))

//     val result = Evidence.resolvePublicationDates(evidenceData, publicationData)

//     // Check that evidenceDate column has DateType
//     val evidenceDateField = result.schema.fields.find(_.name == "evidenceDate").get
//     evidenceDateField.dataType should be(DateType)
//   }

//   "resolvePublicationDates" should "handle empty literature arrays gracefully" in {
//     val evidenceData = Seq(
//       ("evidence1", "2023-01-01", Seq.empty[String]),
//       ("evidence2", "2023-02-01", null.asInstanceOf[Seq[String]])
//     ).toDF("id", "releaseDate", "literature")

//     val publicationData = Seq(
//       ("MED", "2022-06-15", "PMID123", "PMID123", "")
//     ).toDF("source", "firstPublicationDate", "pmid", "id", "pmcid")
//       .withColumn("pmcid", when(col("pmcid") === "", lit(null).cast(StringType)).otherwise(col("pmcid")))

//     // Should not throw an exception
//     val result = Evidence.resolvePublicationDates(evidenceData, publicationData)
//     val resultData = result.collect()

//     resultData should have length 2
//     // Both should fall back to releaseDate
//     resultData.foreach { row =>
//       row.getDate(row.fieldIndex("evidenceDate")) should not be null
//     }
//   }

//   // TODO: Add tests for other Evidence functions
//   "prepare" should "add sourceId column and handle resourceScore" in {
//     val inputData = Seq(
//       ("ds1", 0.5),
//       ("ds2", 0.8)
//     ).toDF("datasourceId", "resourceScore")

//     val result = Evidence.prepare(inputData)

//     result.columns should contain("sourceId")
//     result.select("sourceId").collect().map(_.getString(0)) should contain allOf("ds1", "ds2")
//   }

  // TODO: Add more comprehensive tests for:
  // - resolveTargets
  // - resolveDiseases
  // - excludeByBiotype
  // - generateHashes
  // - score
  // - markDuplicates
  // etc.
}
