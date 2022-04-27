package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.unionDataframeDifferentSchema
import org.apache.spark.sql.functions.{
  array_contains,
  broadcast,
  col,
  collect_set,
  element_at,
  lit,
  split,
  struct,
  trim,
  typedLit,
  when
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TargetSafety(id: String, safetyLiabilities: Array[TargetSafetyEvidence])

case class TargetSafetyStudy(name: String, description: String, `type`: String)

case class TargetSafetyEvidence(
    event: String,
    eventId: String,
    effects: Array[(String, String)],
    biosample: Array[Biosample],
    datasource: String,
    literature: String,
    url: String,
    study: Array[TargetSafetyStudy]
)

case class Biosample(
    tissueLabel: String,
    tissueId: String,
    cellLabel: String,
    cellFormat: String,
    cellId: String
)

object Safety extends LazyLogging {

  def apply(
      adverseEventsDF: DataFrame,
      safetyRiskDF: DataFrame,
      toxicityDF: DataFrame,
      geneToEnsgLookup: DataFrame
  )(implicit sparkSession: SparkSession): Dataset[TargetSafety] = {
    import sparkSession.implicits._

    logger.info("Computing target safety information.")

    // transform raw data frames into desired format
    val aeDF: DataFrame = transformAdverseEvents(adverseEventsDF)
    val tsDF: DataFrame = transformTargetSafety(safetyRiskDF)
    val toxDF: DataFrame =
      transformToxCast(toxicityDF, geneToEnsgLookup)

    // combine into single dataframe and group by Ensembl id.
    // The data is relatively sparse, so expect lots of nulls.
    val combinedDF = unionDataframeDifferentSchema(Seq(aeDF, tsDF, toxDF))
      .groupBy(
        col("event"),
        col("eventId"),
        col("datasource"),
        col("id"),
        col("effects"),
        col("literature"),
        col("url")
      )
      .agg(collect_set(col("study")) as "study", collect_set(col("biosample")) as "biosample")
      .transform(groupByEvidence)

    combinedDF.as[TargetSafety]
  }

  private def transformAdverseEvents(df: DataFrame): DataFrame = {
    logger.debug("Transforming target safety adverse events data.")
    val aeDF = df
      .select(
        col("ensemblId") as "id",
        col("symptom") as "event",
        col("efoId") as "eventId",
        col("ref") as "datasource",
        col("pmid") as "literature",
        col("url"),
        struct(
          col("biologicalSystem") as "tissueLabel",
          col("uberonCode") as "tissueId",
          typedLit[String](null) as "cellLabel",
          typedLit[String](null) as "cellFormat",
          typedLit[String](null) as "cellId"
        ) as "biosample",
        split(col("effect"), "_") as "effects"
      )
      .withColumn(
        "effects",
        struct(
          element_at(col("effects"), 1) as "direction",
          element_at(col("effects"), 2) as "dosing"
        )
      )

    val effectsDF = aeDF
      .groupBy("id", "event", "datasource")
      .agg(collect_set(col("effects")) as "effects")

    aeDF.drop("effects").join(effectsDF, Seq("id", "event", "datasource"), "left_outer")
  }

  private def transformTargetSafety(df: DataFrame): DataFrame = {
    logger.debug("Transforming target safety safety risk data.")
    df.select(
      col("ensemblId") as "id",
      when(col("ref").contains("Force"), "heart disease")
        .when(col("ref").contains("Lamore"), "cardiac arrhythmia") as "event",
      when(col("ref").contains("Force"), "EFO_0003777")
        .when(col("ref").contains("Lamore"), "EFO_0004269") as "eventId",
      struct(
        col("biologicalSystem") as "tissueLabel",
        col("uberonId") as "tissueId",
        typedLit[String](null) as "cellLabel",
        typedLit[String](null) as "cellFormat",
        typedLit[String](null) as "cellId"
      ) as "biosample",
      col("ref") as "datasource",
      col("pmid") as "literature",
      struct(
        typedLit[String](null) as "name",
        col("liability") as "description",
        typedLit[String](null) as "type"
      ) as "study"
    )
  }

  def transformToxCast(toxDF: DataFrame, geneIdLookup: DataFrame): DataFrame = {
    logger.debug("Transforming target safety toxcast data.")
    val tDf = broadcast(
      toxDF.select(
        col("biological_process_target") as "event",
        col("eventId"),
        struct(
          col("tissue") as "tissueLabel",
          typedLit[String](null) as "tissueId",
          col("cell_short_name") as "cellLabel",
          col("cell_format") as "cellFormat",
          lit("") as "cellId"
        ) as "biosample",
        trim(col("official_symbol")) as "official_symbol",
        lit("ToxCast") as "datasource",
        lit(
          "https://www.epa.gov/chemical-research/exploring-toxcast-data-downloadable-data"
        ) as "url",
        struct(
          col("assay_component_endpoint_name") as "name",
          col("assay_component_desc") as "description",
          col("assay_format_type") as "type"
        ) as "study"
      )
    ).join(geneIdLookup, array_contains(col("name"), col("official_symbol")), "left_outer")
      .drop(geneIdLookup.columns.filter(_ != "ensgId"): _*)
      .withColumnRenamed("ensgId", "id")

    tDf
  }

  private def groupByEvidence(df: DataFrame): DataFrame = {
    logger.debug("Grouping target safety by ensembl id.")

    df.select(
      col("id"),
      struct(
        col("event"),
        col("eventId"),
        col("effects"),
        col("biosample"),
        col("datasource"),
        col("literature"),
        col("url"),
        col("study")
      ) as "safety"
    ).groupBy("id")
      .agg(collect_set("safety") as "safetyLiabilities")
  }
}
