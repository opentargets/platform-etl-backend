package io.opentargets.etl.backend

import org.apache.spark.sql.SparkSession

trait SparkSessionSetup {

  lazy implicit val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("etlSparkTest")
    .config("spark.driver.maxResultSize", "0")
    .getOrCreate()

}

