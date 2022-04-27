package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers._
import io.opentargets.etl.backend.target.TargetUtils.transformArrayToStruct
import org.apache.spark.sql.functions.{col, collect_list, explode, split, typedLit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** @param id        ensembl gene id eg. ENSGXXXX
  * @param locations subcellular locations
  */
case class GeneWithLocation(id: String, locations: Seq[LocationAndSource])

object GeneWithLocation extends LazyLogging {

  /** @param df            file from HPA
    * @param slLocationsDf file provided by data team with mappings from HPA location to subcellular location ontology IDs.
    */
  def apply(df: DataFrame, slLocationsDf: DataFrame)(implicit
      sparkSession: SparkSession
  ): Dataset[GeneWithLocation] = {
    import sparkSession.implicits._

    logger.info("Transforming HPA inputs.")

    val locationAndSourceStruct: StructType = StructType(
      Array(
        StructField("location", StringType),
        StructField("source", StringType)
      )
    )
    val hpaDf = df
      .select(
        col("Gene").as("id"),
        split(col("Main location"), ";").as("HPA_main"),
        split(col("Additional location"), ";").as("HPA_additional"),
        split(col("Extracellular location"), ";").as("HPA_extracellular_location")
      )
      .withColumn(
        "HPA_main",
        transformArrayToStruct(
          col("HPA_main"),
          typedLit("HPA_main") :: Nil,
          locationAndSourceStruct
        )
      )
      .withColumn(
        "HPA_additional",
        transformArrayToStruct(
          col("HPA_additional"),
          typedLit("HPA_additional") :: Nil,
          locationAndSourceStruct
        )
      )
      .withColumn(
        "HPA_extracellular_location",
        transformArrayToStruct(
          col("HPA_extracellular_location"),
          typedLit("HPA_extracellular_location") :: Nil,
          locationAndSourceStruct
        )
      )
      .withColumn(
        "locations",
        explode(
          safeArrayUnion(col("HPA_main"), col("HPA_additional"), col("HPA_extracellular_location"))
        )
      )
      .select(col("id"), col("locations.location"), col("locations.source"))
      .join(slLocationsDf, col("location") === col("HPA_location"), "left_outer")
      .transform(nest(_: DataFrame, List("location", "source", "termSL", "labelSL"), "locations"))
      .groupBy("id")
      .agg(collect_list(col("locations")) as "locations")

    hpaDf.as[GeneWithLocation].map(identity)
  }
}
