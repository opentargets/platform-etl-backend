package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers._
import io.opentargets.etl.backend.target.TargetUtils.transformArrayToStruct
import org.apache.spark.sql.functions.{col, split, typedLit}
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
        split(col("Main location"), ";").as("HPA_main"),
        split(col("Additional location"), ";").as("HPA_additional"),
        split(col("Extracellular location"), ";").as("HPA_extracellular_location"),
      )
      .withColumn("HPA_main",
                  transformArrayToStruct(col("HPA_main"),
                                         typedLit("HPA_main") :: Nil,
                                         locationAndSourceSchema))
      .withColumn("HPA_additional",
                  transformArrayToStruct(col("HPA_additional"),
                                         typedLit("HPA_additional") :: Nil,
                                         locationAndSourceSchema))
      .withColumn(
        "HPA_extracellular_location",
        transformArrayToStruct(col("HPA_extracellular_location"),
                               typedLit("HPA_extracellular_location") :: Nil,
                               locationAndSourceSchema)
      )
      .withColumn(
        "locations",
        safeArrayUnion(col("HPA_main"), col("HPA_additional"), col("HPA_extracellular_location")))

    hpaDf.select(col("id"), col("locations")).as[GeneWithLocation]
  }
}
