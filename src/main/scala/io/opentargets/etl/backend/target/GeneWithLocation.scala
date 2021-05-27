package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.safeArrayUnion
import io.opentargets.etl.backend.target.TargetUtils.transformColumnToLabelAndSourceStruct
import org.apache.spark.sql.functions.{col, split}
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
    val mainDF = hpaDf.transform(locationToSourceAndLocation("HPA_main"))
    val additionalDF = hpaDf.transform(locationToSourceAndLocation("HPA_additional"))
    val extracellularDF = hpaDf.transform(locationToSourceAndLocation("HPA_extracellular_location"))

    val geneWithLocationDF = List(hpaDf.select("id"), mainDF, additionalDF, extracellularDF)
      .reduce((a, b) => a.join(b, Seq("id"), "left_outer"))
      .select(col("id"),
              safeArrayUnion(col("HPA_main"),
                             col("HPA_additional"),
                             col("HPA_extracellular_location")).as("locations"))

    geneWithLocationDF.as[GeneWithLocation]

  }

  private def locationToSourceAndLocation(location: String)(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(col("id"), col(location))
      .transform(
        transformColumnToLabelAndSourceStruct(_, "id", location, location, Some("location")))
  }
}
