package io.opentargets.etl.backend.drug_beta

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.Seq
import scala.io.Source

/**
  * Object to process ChEMBL indications for incorporation into Drug.
  *
  * Output schema:
  * id
  * indications
  * -- count
  * -- rows
  * ---- disease
  * ---- maxPhaseForIndication
  * ---- references
  * ------ source
  * ------ ids
  * ------ urls
  * indication_therapeutic_areas
  * -- therapeutic_code
  * -- therapeutic_label
  * -- count
  */
object Indication extends Serializable with LazyLogging {

  def apply(indicationsRaw: DataFrame, efoRaw: DataFrame, efoCurated: DataFrame)(implicit ss: SparkSession): DataFrame = {

    logger.info("Processing indications.")
    // efoDf for therapeutic areas
    val efoDf = getEfoDataframe(efoRaw)
    val indicationAndEfoDf = processIndicationsRawData(indicationsRaw, efoCurated)
      .join(efoDf, Seq("efo_id"), "leftouter")

    val indicationDf: DataFrame = indicationAndEfoDf
      .withColumn("struct",
        struct(col("efo_id").as("disease"),
          col("max_phase_for_indications").as("maxPhaseForIndication"),
          col("references")))
      .groupBy("id")
      .agg(collect_list("struct").as("rows"))
      .withColumn("count", size(col("rows")))
      .transform(nest(_: DataFrame, List("rows", "count"), "indications"))

    indicationDf
  }

  /**
    *
    * @param rawEfoData taken from the `disease` input data
    * @return dataframe of `efo_id`, `efo_label`, `efo_uri`, `therapeutic_codes`, `therapeutic_labels`
    */
  private def getEfoDataframe(rawEfoData: DataFrame): DataFrame = {
    val columnsOfInterest = Seq(("code", "efo_url"),
      ("label", "efo_label"),
      ("therapeutic_codes", "therapeutic_codes"),
      ("therapeutic_labels", "therapeutic_labels"))
    val df = rawEfoData
      .select(columnsOfInterest.map(_._1).map(col): _*)
      .withColumn("efo_id", Helpers.stripIDFromURI(col("code")))
    // rename columns and make sure efo_id is correct 'shape'
    columnsOfInterest.foldLeft(df)((d, names) => d.withColumnRenamed(names._1, names._2)).transform(formatEfoIds)
  }

  private def processIndicationsRawData(indicationsRaw: DataFrame, curatedDf: DataFrame): DataFrame = {

    val df = formatEfoIds(indicationsRaw)

    // flatten hierarchy
    val dfWithFlatHierarchy = df.withColumn("r", explode(col("indication_refs")))
      .select(col("molecule_chembl_id").as("id"),
        col("efo_id"),
        col("max_phase_for_ind"),
        col("r.ref_id"),
        col("r.ref_type"),
        col("r.ref_url"))
      // handle case where clinical trials packs multiple ids into a csv string
      .withColumn("ref_id", explode(split(col("ref_id"), ",")))

    // Try and match any records with null EFO fields with the curated list.
    val withCuratedEfos = dfWithFlatHierarchy
      .filter(col("efo_id").isNull)
      .drop("efo_id")
      .join(curatedDf.withColumnRenamed("molecule_chembl_id", "id"), Seq("id", "ref_id"))

    dfWithFlatHierarchy.filter(col("efo_id").isNotNull) // prevent duplicates
      .unionByName(withCuratedEfos)
      // group reference ids and urls by ref_type
      .groupBy("id", "efo_id", "ref_type")
      .agg(max("max_phase_for_ind").as("max_phase_for_ind"),
        collect_list("ref_id").as("ids"),
        collect_list("ref_url").as("urls"))
      // nest references and find max_phase
      .withColumn("references",
        struct(
          col("ref_type").as("source"),
          col("ids"),
          col("urls")
        ))
      .groupBy("id", "efo_id")
      .agg(
        max("max_phase_for_ind").as("max_phase_for_indications"),
        collect_list("references").as("references")
      )
  }

  /**
    *
    * @param indicationDf dataframe of ChEMBL indications
    * @return dataframe with efo_ids in form EFO_xxxx instead of EFO:xxxx
    */
  private def formatEfoIds(indicationDf: DataFrame): DataFrame = {
    indicationDf.withColumn("efo_id", regexp_replace(col("efo_id"), ":", "_"))
  }

}
