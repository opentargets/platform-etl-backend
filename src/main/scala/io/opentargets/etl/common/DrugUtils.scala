package io.opentargets.etl.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DrugUtils {

  /** Obtains a lookup table of drug name and ids and it takes only oen id when more than one is
    * present for the same name. It also normalizes the names to lower case.
    * @param drugsDF
    *   Data Frame containing drug information
    * @return
    *   Lookup table with drug name and id columns
    */
  private def getDrugNameLut(drugsDF: DataFrame): DataFrame =
    drugsDF
      .select(col("id"), lower(col("name")).as("drugFromSource"))
      .groupBy(col("drugFromSource"))
      .agg(collect_set(col("id")).as("ids"))
      .select(col("drugFromSource"),
              element_at(sort_array(col("ids"), asc = false), 1).as("drugIdCross")
      )

  private def completeByDrugName(mapToDF: DataFrame, moleculeDF: DataFrame): DataFrame = {

    val namesLutDF = getDrugNameLut(moleculeDF)

    mapToDF
      .withColumn("drugFromSource", lower(col("drugFromSource")))
      .join(namesLutDF, Seq("drugFromSource"), "left")
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

    val drugNameDF = completeByDrugName(mapToDF, moleculeDF)

    drugNameDF
      .withColumnRenamed("drugIdCross", "drugId")
      .drop("drugIdCross")

  }

}
