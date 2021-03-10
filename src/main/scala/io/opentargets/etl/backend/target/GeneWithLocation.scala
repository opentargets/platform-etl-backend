package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.mkFlattenArray
import io.opentargets.etl.backend.target.TargetUtils.transformColumnToLabelAndSourceStruct
import org.apache.spark.sql.functions.{col, collect_list, explode, split}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @param id        ensembl gene id eg. ENSGXXXX
  * @param locations subcellular locations
  */
case class GeneWithLocation(id: String, locations: Seq[LocationAndSource])

object GeneWithLocation extends LazyLogging {
  def apply(df: DataFrame)(implicit sparkSession: SparkSession): Dataset[GeneWithLocation] = {
    import sparkSession.implicits._

    logger.info("Transforming HPA inputs.")
    val hpaDf = df
      .select(
        col("Gene").as("id"),
        col("Main location").as("l1"),
        col("Additional location").as("l2"),
        col("Extracellular location").as("l3"),
      )
      .withColumn(
        "l",
        mkFlattenArray(split(col("l1"), ";"), split(col("l2"), ";"), split(col("l3"), ";")))
      .select(col("id"), explode(col("l")).as("l"))
      .groupBy(col("id"))
      .agg(collect_list(col("l")).as("locations"))
      .transform(
        transformColumnToLabelAndSourceStruct(_, "id", "locations", "HPA", Some("location")))
      .as[GeneWithLocation]

    hpaDf

  }
}
