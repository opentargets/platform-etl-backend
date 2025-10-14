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
  val evidenceSchema = StructType(
    Array(
      StructField("id", StringType, nullable = false),
      StructField("releaseDate", StringType, nullable = true),
      StructField("literature", ArrayType(StringType), nullable = true)
    )
  )

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

  // Testing evidence date priorisation:
  val df1 = sparkSession.createDataFrame(
    sparkSession.sparkContext.parallelize(
      Seq(
        Row("1", "2021-06-15", "2022-05-12", "1996-05-12"),
      )
    ),
    StructType(
      Array(
        StructField("id", StringType, nullable = false),
        StructField("studyStartDate", StringType, nullable = true),
        StructField("publicationDate", StringType, nullable = true),
        StructField("releaseDate", StringType, nullable = true),
      )
    )
  )

  val df2 = sparkSession.createDataFrame(
    sparkSession.sparkContext.parallelize(
      Seq(
        Row("1", "2021-06-15", "2022-05-12", null),
      )
    ),
    StructType(
      Array(
        StructField("id", StringType, nullable = false),
        StructField("studyStartDate", StringType, nullable = true),
        StructField("publicationDate", StringType, nullable = true),
        StructField("releaseDate", StringType, nullable = true),
      )
    )
  )

  val df3 = sparkSession.createDataFrame(
    sparkSession.sparkContext.parallelize(
      Seq(
        Row("1", "1996-05-12"),
      )
    ),
    StructType(
      Array(
        StructField("id", StringType, nullable = false),
        StructField("releaseDate", StringType, nullable = true),
      )
    )
  )

  val df4 = sparkSession.createDataFrame(
    sparkSession.sparkContext.parallelize(
      Seq(
        Row("1"),
      )
    ),
    StructType(
      Array(
        StructField("id", StringType, nullable = false),
      )
    )
  )

  // Apply the function using shared test data
  val result = Evidence.resolvePublicationDates(testEvidenceData, testPublicationData)

  "resolvePublicationDates" should "return dataframe" in {

    // Compile-time type assertion
    implicitly[result.type <:< DataFrame]

  }

  it should "return all evidence" in {
    // Test that DataFrame is created successfully
    result.count() should be(5)

    // Should have all expected columns
    val expectedColumns = testEvidenceData.columns
    result.columns should contain allElementsOf (expectedColumns)

  }

  it should "have new `publicationDate` column with the right type" in {
    // Test for new column:
    result.columns should contain("publicationDate")

    // Test column schema:
    result.schema("publicationDate").dataType should be(StringType)
    result.schema("publicationDate").nullable should be(true)
  }

  it should "have new `evidenceDate` column with the right type" in {
    // Test for new column:
    result.columns should contain("evidenceDate")

    // Test column schema:
    result.schema("evidenceDate").dataType should be(StringType)
    result.schema("evidenceDate").nullable should be(true)
  }

  it should "correctly resolve publication dates for specific evidence" in {
    // Test specific evidence records

    // e1: No literature, no releaseDate - publicationDate should be null, evidenceDate should be null
    val evidence1 = result.filter(col("id") === "e1").collect().head
    evidence1.getString(evidence1.fieldIndex("publicationDate")) should be(null)
    evidence1.getString(evidence1.fieldIndex("evidenceDate")) should be(null)

    // e2: No literature, has releaseDate - publicationDate should be null, evidenceDate should be releaseDate
    val evidence2 = result.filter(col("id") === "e2").collect().head
    evidence2.getString(evidence2.fieldIndex("publicationDate")) should be(null)
    evidence2.getString(evidence2.fieldIndex("evidenceDate")) should be("2021-02-03")

    // e3: Has literature that can be resolved - publicationDate should be from literature, evidenceDate should prioritize publication date
    val evidence3 = result.filter(col("id") === "e3").collect().head
    evidence3.getString(evidence3.fieldIndex("publicationDate")) should be("2021-06-15")
    evidence3.getString(evidence3.fieldIndex("evidenceDate")) should be("2021-02-03")

    // e4: Has literature that can be resolved - publicationDate should be from literature, evidenceDate should prioritize publication date
    val evidence4 = result.filter(col("id") === "e4").collect().head
    evidence4.getString(evidence4.fieldIndex("publicationDate")) should be("2021-06-15")
    evidence4.getString(evidence4.fieldIndex("evidenceDate")) should be("2021-06-15")

    // e5: Has literature that can be resolved - publicationDate should be from literature, which is an older date
    val evidence5 = result.filter(col("id") === "e5").collect().head
    evidence5.getString(evidence5.fieldIndex("publicationDate")) should be("2021-08-15")
    evidence5.getString(evidence5.fieldIndex("evidenceDate")) should be("2021-08-15")

  }

  // Test different use cases:
  it should "robustly pick earliest date of arbitrary columns" in {
      
    val filtered1 = df1.select(
      Evidence.minDate(
        df1,
        "studyStartDate",  "publicationDate", "releaseDate"
      ).alias("evidenceDate")
    ).first()
    filtered1.getAs[String]("evidenceDate") should be("1996-05-12")

    val filtered2 = df2.select(
      Evidence.minDate(
        df2,
        "studyStartDate",  "publicationDate", "releaseDate"
      ).alias("evidenceDate")
    ).first()
    filtered2.getAs[String]("evidenceDate") should be("2021-06-15")

    val filtered3 = df3.select(
      Evidence.minDate(
        df3,
        "studyStartDate",  "publicationDate", "releaseDate"
      ).alias("evidenceDate")
    ).first()
    filtered3.getAs[String]("evidenceDate") should be("1996-05-12")

    val filtered4 = df4.select(
      Evidence.minDate(
        df4,
        "studyStartDate",  "publicationDate", "releaseDate"
      ).alias("evidenceDate")
    ).first()
    filtered4.getAs[String]("evidenceDate") should be(null)

  }
}