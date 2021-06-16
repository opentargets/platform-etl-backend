package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Object to process ChEMBL indications for incorporation into Drug.
  *
  * Output schema:
  * ids
  * maxPhaseForIndication
  * references
  * -- source
  * -- ids
  * efoName
  * disease
  */
object Indication extends Serializable with LazyLogging {
  private val efoId: String = "disease"

  def apply(indicationsRaw: DataFrame, efoRaw: DataFrame)(implicit ss: SparkSession): DataFrame = {
    logger.info("Processing indications.")
    // efoDf for therapeutic areas
    val efoDf = getEfoDataframe(efoRaw)
    val indicationDf = processIndicationsRawData(indicationsRaw)

    // add disease name and replace obsolete EfoId with new term.
    indicationDf
      .join(efoDf, array_contains(efoDf("allEfoIds"), indicationDf(efoId)))
      .drop("allEfoIds", efoId)
      .withColumnRenamed("updatedEfo", efoId)
  }

  /**
    *
    * @param rawEfoData taken from the `disease` input data
    * @return dataframe of `updatedEfo`, `efoName`, `allEfoIds`
    */
  private def getEfoDataframe(rawEfoData: DataFrame): DataFrame = {
    rawEfoData
      .select(
        col("id") as "updatedEfo",
        col("name"),
        array_union(array(col("id")), coalesce(col("obsoleteTerms"), array())) as "allEfoIds"
      )
      .select(
        transform(col("allEfoIds"), it => translate(it, ":", "_")) as "allEfoIds",
        trim(lower(col("name"))).as("efoName"),
        col("updatedEfo")
      )

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
