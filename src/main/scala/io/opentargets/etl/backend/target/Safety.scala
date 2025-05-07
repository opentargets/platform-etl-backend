package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{array_contains, coalesce, col, collect_set, explode, struct}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TargetSafety(id: String, safetyLiabilities: Array[TargetSafetyEvidence])

case class TargetSafetyStudy(name: String, description: String, `type`: String)

case class TargetSafetyEvidence(
    event: String,
    eventId: String,
    effects: Array[(String, String)],
    biosamples: Array[Biosample],
    datasource: String,
    literature: String,
    url: String,
    studies: Array[TargetSafetyStudy]
)

case class Biosample(
    tissueLabel: String,
    tissueId: String,
    cellLabel: String,
    cellFormat: String
)

object Safety extends LazyLogging {

  def apply(
      safetyEvidence: DataFrame,
      geneToEnsgLookup: DataFrame,
      diseasesDf: DataFrame
  )(implicit sparkSession: SparkSession): Dataset[TargetSafety] = {
    import sparkSession.implicits._

    logger.info("Computing target safety information.")

    val safetyWithEnsgIds: DataFrame =
      addMissingGeneIdsToSafetyEvidence(safetyEvidence, geneToEnsgLookup)

    val safetyGroupedById = safetyWithEnsgIds
      .transform(replaceObsoleteEFOs(diseasesDf, _))
      .transform(groupByEvidence)

    safetyGroupedById.as[TargetSafety]
  }

  /** The safety dataset is provided by the data team: entries for Toxcast do not have a valid ENSG
    * ID and cannot be mapped to the correct target object. The gene name is included in the field
    * `targetFromSourceId` and can be used to correctly map the toxcast evidence to the correct
    * target.
    * @param safetyDf
    *   provided raw from the data team.
    * @param geneIdLookup
    * @return
    *   dataframe with each evidence string mapped to a valid target.
    */
  def addMissingGeneIdsToSafetyEvidence(safetyDf: DataFrame, geneIdLookup: DataFrame): DataFrame = {
    logger.debug("Adding missing ENSG IDs to toxcast entries.")
    // join on name, merge ensgId from lookup with id from df
    val tDf = safetyDf
      .join(geneIdLookup, array_contains(col("name"), col("targetFromSourceId")), "left_outer")
      .drop(geneIdLookup.columns.filter(_ != "ensgId"): _*)
      .withColumn("temp_id", coalesce(col("id"), col("ensgId")))
      .drop("id", "ensgId")
      .withColumnRenamed("temp_id", "id")

    logger.whenDebugEnabled({
      val unmappedToxcastCount =
        tDf.filter(col("datasource") === "ToxCast" && col("id").isNull).count
      logger.debug(s"There were $unmappedToxcastCount with no valid target mapping.")
    })

    tDf
  }

  /** Creates a LUT from the dieseases input to replace the obsolete EFOs in the safety evidence
    * data,
    *
    * @param inputDataFrames
    *   map with input dataframes
    * @return
    *   Safety Evidence DataFrame with obsolete EFOs replaced
    */
  private def replaceObsoleteEFOs(diseasesDf: DataFrame, safety: DataFrame): DataFrame = {
    logger.info("Replacing obsolete EFOs in gene ontology using diseases data")

    val diseaseMappingDf =
      diseasesDf.select(col("id").as("diseaseId"), explode(col("obsoleteTerms")).as("obsoleteTerm"))

    safety
      .join(diseaseMappingDf, col("eventId") === diseaseMappingDf.col("obsoleteTerm"), "left_outer")
      .withColumn("eventId", coalesce(col("diseaseId"), col("eventId")))
      .drop("obsoleteTerm", "diseaseId")
  }

  // TODO: Move replaceObsoleteEFOs to this object
  private def groupByEvidence(df: DataFrame): DataFrame = {
    logger.debug("Grouping target safety by ensembl id.")
    df.select(
      col("id"),
      struct(
        col("event"),
        col("eventId"),
        col("effects"),
        col("biosamples"),
        col("datasource"),
        col("literature"),
        col("url"),
        col("studies")
      ) as "safety"
    ).groupBy("id")
      .agg(collect_set("safety") as "safetyLiabilities")
  }
}
