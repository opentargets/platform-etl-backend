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
  * ------ urls
  */
object Indication extends Serializable with LazyLogging {
  private val efoIdName: String = "efo_id"

  def apply(indicationsRaw: DataFrame, efoRaw: DataFrame)(implicit ss: SparkSession): DataFrame = {

    logger.info("Processing indications.")
    // efoDf for therapeutic areas
    val efoDf = getEfoDataframe(efoRaw, "id", efoIdName)
    val indicationAndEfoDf = processIndicationsRawData(indicationsRaw)
      .join(efoDf, Seq(efoIdName), "leftouter")

    val indicationDf: DataFrame = indicationAndEfoDf
      .withColumn("struct",
        struct(col(efoIdName).as("disease"),
          col("max_phase_for_indications").as("maxPhaseForIndication"),
          col("references")))
      .groupBy("id")
      .agg(collect_list("struct").as("rows"))
      .withColumn("count", size(col("rows")))
      .transform(nest(_: DataFrame, List("rows", "count"), "indications"))
      .join(approvedIndications(indicationAndEfoDf), Seq("id"), "left_outer")

    indicationDf
  }
  /**
    *
    * @param rawEfoData taken from the `disease` input data
    * @return dataframe of `efo_id`
    */
  private def getEfoDataframe(rawEfoData: DataFrame, fromCol: String, toCol: String): DataFrame = {
    val columnsOfInterest = Seq(
      col(fromCol).as(toCol)
    )

    rawEfoData
      .select(columnsOfInterest: _*)
      .transform(formatEfoIds(_, toCol))
  }

  private def processIndicationsRawData(indicationsRaw: DataFrame): DataFrame = {

    val df = formatEfoIds(indicationsRaw, efoIdName)

    // flatten hierarchy
    df.withColumn("r", explode(col("indication_refs")))
      .select(col("molecule_chembl_id").as("id"),
        col(efoIdName),
        col("max_phase_for_ind"),
        col("r.ref_id"),
        col("r.ref_type"),
        col("r.ref_url"))
      // remove indications we can't link to a disease.
      .filter(col(efoIdName).isNotNull)
      // handle case where clinical trials packs multiple ids into a csv string
      .withColumn("ref_id", split(col("ref_id"), ","))
      .withColumn("ref_id", explode(col("ref_id")))
      // group reference ids and urls by ref_type
      .groupBy("id", efoIdName, "ref_type")
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
      .groupBy("id", efoIdName)
      .agg(
        max("max_phase_for_ind").as("max_phase_for_indications"),
        collect_list("references").as("references")
      )
  }

  /**
    *
    * @param indicationDf dataframe of ChEMBL indications
    * @param idCol the column to be used as ID
    * @return dataframe with efo_ids in form EFO_xxxx instead of EFO:xxxx
    */
  private def formatEfoIds(indicationDf: DataFrame, idCol: String): DataFrame = {
    indicationDf.withColumn(efoIdName, translate(col(idCol), ":", "_"))
  }

  private def approvedIndications(df: DataFrame): DataFrame =
    df.select("id", efoIdName, "max_phase_for_indications")
      .filter("max_phase_for_indications = 4")
      .groupBy("id")
      .agg(collect_set(col(efoIdName)).as("approvedIndications"))

}
