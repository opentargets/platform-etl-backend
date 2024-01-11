package io.opentargets.etl.backend.drug

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.functions.{col, collect_list, collect_set, flatten, lit, split, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Drug warnings as produced by ChEMBL. Available since ChEMBL release 28.
  *
  * Drug warning are manually curated by ChEMBL according to the methodology outlined
  * [[https://pubs.acs.org/doi/pdf/10.1021/acs.chemrestox.0c00296 in this research paper]].
  */
object DrugWarning extends LazyLogging {
  def apply(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    logger.info("Preparing drug warnings")
    val warningsDF = df.selectExpr(
      "_metadata.all_molecule_chembl_ids as chemblIds",
      "warning_class as toxicityClass",
      "warning_country as country",
      "warning_description as description",
      "warning_id as id",
      "warning_refs as references",
      "warning_type as warningType",
      "warning_year as year",
      "efo_term",
      "efo_id",
      "efo_id_for_warning_class"
    )
    warningsDF
  }

  def processWithdrawnNotices(warningDF: DataFrame): DataFrame = {
    val withdrawnDf = warningDF
      .filter(col("warning_type") === "Withdrawn")
      .withColumn("warning_country", split(col("warning_country"), ";"))
      .withColumn("warning_class", split(col("warning_class"), ";"))

    withdrawnDf
      .select(
        col("molecule_chembl_id") as "id",
        lit(true) as "isWithdrawn"
      )
      .distinct()
  }

}
