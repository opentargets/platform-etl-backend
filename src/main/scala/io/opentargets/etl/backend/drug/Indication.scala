package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Object to process ChEMBL indications for incorporation into Drug.
  *
  * Output schema:
  * id
  * approvedIndications
  * indications
  * -- count
  * -- rows
  * ---- disease
  * ---- maxPhaseForIndication
  * ---- references
  * ------ source
  * ------ ids
  */
object Indication extends Serializable with LazyLogging {
  private val efoIdName: String = "disease"

  def apply(indicationsRaw: DataFrame, efoRaw: DataFrame)(implicit ss: SparkSession): DataFrame = {
    logger.info("Processing indications.")
    // efoDf for therapeutic areas
    val efoDf = getEfoDataframe(efoRaw)
    processIndicationsRawData(indicationsRaw).join(efoDf, Seq(efoIdName))
  }

  /**
    *
    * @param rawEfoData taken from the `disease` input data
    * @return dataframe of `efo_id`, `efoName`
    */
  private def getEfoDataframe(rawEfoData: DataFrame): DataFrame = {
    // there are obsolete EFOs in the ChEMBL data so we need to create a table with obsolete
    // terms included in the list of possible ids.
    rawEfoData
      .select(array_union(array(col("id")), coalesce(col("obsoleteTerms"), array())) as "ids",
              col("name"))
      .select(explode(col("ids")) as efoIdName, col("name"))
      .select(translate(col(efoIdName), ":", "_") as efoIdName,
              trim(lower(col("name"))).as("efoName"))
  }

  /**
    *
    * @param indicationsRaw data as provided by ChEMBL
    * @return dataframe with columns: ids, references, maxPhaseForIndication, disease
    */
  private def processIndicationsRawData(indicationsRaw: DataFrame): DataFrame = {

    val maxP = "maxPhaseForIndication"
    val ref = "references"
    indicationsRaw
      .select(
        col("_metadata.all_molecule_chembl_ids") as "ids",
        explode(col("indication_refs")) as ref,
        col("max_phase_for_ind") as maxP,
        translate(col("efo_id"), ":", "_") as "disease"
      )
      .withColumn("ref_id", split(col(s"$ref.ref_id"), ","))
      .withColumn("source", col(s"$ref.ref_type"))
      .drop(ref)
      // group reference ids by source
      .groupBy("ids", maxP, "disease", "source")
      .agg(collect_set("ref_id") as "ref_id")
      // create structure of references and group
      .withColumn(ref,
                  struct(
                    col("source"),
                    flatten(col("ref_id")) as "ids"
                  ))
      .groupBy("ids", maxP, "disease")
      .agg(collect_set(col(ref)) as ref)
  }
}
