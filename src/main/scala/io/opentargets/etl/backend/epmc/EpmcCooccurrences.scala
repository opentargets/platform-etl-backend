package io.opentargets.etl.backend.epmc

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, collect_set, concat, lit, struct, when}

object EpmcCooccurrences extends LazyLogging {

  def generateUri(keywordId: Column)(implicit context: ETLSessionContext): Column = {
    val uris = context.configuration.epmc.uris
    when(keywordId.startsWith("ENSG"), concat(lit(uris.ensembl), keywordId))
      .when(keywordId.startsWith("CHEMBL"), concat(lit(uris.chembl), keywordId))
      .otherwise(concat(lit(uris.ontologies), keywordId))
  }

  private def mapCoocurrenceType(cType: Column): Column =
    when(cType === "DS-CD", lit("Disease Drug Relationship"))
      .when(cType === "GP-CD", lit("Gene Drug Relationship"))
      .when(cType === "GP-DS", lit("Gene Disease Relationship"))

  def apply(cooccurencesDf: DataFrame)(implicit context: ETLSessionContext): DataFrame =
    cooccurencesDf
      .select(
        when(col("pmcid").isNotNull, lit("PMC"))
          .otherwise(lit("MED"))
          .as("src"),
        when(col("pmcid").isNotNull, col("pmcid"))
          .otherwise(col("pmid"))
          .as("id"),
        mapCoocurrenceType(col("type")).as("type"),
        col("text").as("exact"),
        col("section").as("section"),
        array(
          struct(col("label1").as("name"), generateUri(col("keywordId1")).as("uri")),
          struct(col("label2").as("name"), generateUri(col("keywordId2")).as("uri"))
        ).as("tags")
      )
      .groupBy("src", "id")
      .agg(
        collect_set(
          struct(
            col("type"),
            col("exact"),
            col("section"),
            col("tags")
          )
        ).as("anns")
      )
      .withColumn("provider", lit("OpenTargets"))
      .repartition(1)
      .persist()

}
