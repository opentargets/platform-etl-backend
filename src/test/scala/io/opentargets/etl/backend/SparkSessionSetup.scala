package io.opentargets.etl.backend

import org.apache.spark.sql.SparkSession

trait SparkSessionSetup {

  def withSparkSession(testMethod: SparkSession => Any) {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("testSparkTrait")
      .config("spark.driver.maxResultSize", "0")
      .getOrCreate()
    try {
      testMethod(spark)
    } finally spark.stop()
  }
}

