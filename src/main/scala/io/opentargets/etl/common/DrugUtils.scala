package io.opentargets.etl.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DrugUtils {

  private def completeByChebi(mapToDF: DataFrame, lutDF: DataFrame): DataFrame = {
    val chebiLutDF = lutDF.select(
      col("id").alias("drugIdCrossChebi"),
      col("drugFromSourceId"))
    mapToDF.join(chebiLutDF, Seq("drugFromSourceId"), "left")
  }

  private def completeByDrugName(mapToDF: DataFrame, lutDF: DataFrame): DataFrame = {
    val namesLutDF = lutDF.select(
      col("drugFromSource"),
      col("id").as("drugIdCross")
    ).distinct()

    mapToDF
      .withColumn("drugFromSource", lower(col("drugFromSource")))
      .join(namesLutDF, Seq("drugFromSource"), "left")
  }

  /** Obtains a lookup table of drug name and CHEBI id
    * @param drugsDF
    *   Data Frame containing drug information
    * @return
    *   Lookup table with drug name and CHEBI id
    */
  private def getDrugLut(drugsDF: DataFrame): DataFrame = {
    drugsDF
      .select(col("id"), explode_outer(col("crossReferences")), lower(col("name")).as("drugFromSource"))
      .withColumn("drugId", explode_outer(col("value")))
      .select(
        col("id"),
        when(
          col("key") === "chEBI", concat(lit("CHEBI_"),
          col("drugId"))).otherwise(null).as("drugFromSourceId"),
        col("drugFromSource")
      )
      .distinct()
      .groupBy(col("drugFromSource"))
      .agg(collect_set(col("id")).as("ids"), collect_set(col("drugFromSourceId")).as("idsChebi"))
      .select(
        element_at(sort_array(col("ids"), asc = false), 1).as("id"),
        col("drugFromSource"),
        explode_outer(col("idsChebi")).as("drugFromSourceId")
      )
  }

  /** MapDrugId is a function that attempts to map the drug Id from the molecule Data Frame to the
    * mapToDF Data Frame first using the name of the drug and then using the CHEBI id
    * @param mapToDF
    *   Data Frame to map the drug id to
    * @param moleculeDF
    *   Molecules Data Frame to get the drug id
    * @return
    *   the mapToDF with the drugId mapped
    */
  def MapDrugId(mapToDF: DataFrame, moleculeDF: DataFrame): DataFrame = {

    val drugLutDF = getDrugLut(moleculeDF)

    val drugNameDF = completeByDrugName(mapToDF, drugLutDF)

    val nonResolvedByNameDF =
      drugNameDF.where(col("drugIdCross").isNull && col("drugFromSourceId").isNotNull)

    val mergedByChebiDF = completeByChebi(nonResolvedByNameDF, drugLutDF)

    val resolvedByNameDF = drugNameDF
      .where(col("drugIdCross").isNotNull || col("drugFromSourceId").isNull)
      .select(col("*"), lit(null).as("drugIdCrossChebi"))

    val fullDF = resolvedByNameDF.unionByName(mergedByChebiDF, allowMissingColumns = true)

    fullDF
      .select(col("*"), coalesce(col("drugIdCross"), col("drugIdCrossChebi")).as("drugId"))
      .drop("drugIdCrossChebi", "drugIdCross")
  }

}
