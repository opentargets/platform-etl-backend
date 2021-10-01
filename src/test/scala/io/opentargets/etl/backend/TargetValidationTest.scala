package io.opentargets.etl.backend

import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TargetValidationTest extends EtlSparkUnitTest {

  "Validate" should "partition a dataframe based on whether an ENSG ID can be found in the target dataset" in {
    import sparkSession.implicits._
    // given
    implicit val targetDF: DataFrame = 1 to 10 by 2 map { i =>
      (s"ENSG$i", i)
    } toDF ("id", "row")
    val filterColumn = "fid"
    val dfToFilter = 1 to 10 map { i =>
      (s"ENSG$i", i)
    } toDF (filterColumn, "data")

    // when
    val results = TargetValidation.validate(dfToFilter, filterColumn)

    /*
     then the dataframes should be the same size since we have 10 unique targets and 5 unique other results, so 5 results
     should be validated, and 5 results are listed as missing.
     */
    results._1.count() should equal(results._2.count)
  }
}
