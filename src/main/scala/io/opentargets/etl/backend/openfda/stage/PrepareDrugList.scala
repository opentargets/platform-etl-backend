package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, array_distinct, col, explode, flatten, lower}

object PrepareDrugList extends LazyLogging {
  def apply(dfChembl: DataFrame) = {
    logger.info("Prepare ChEMBL data for attaching it to FAERS dataset")
    val drugList = dfChembl
      .selectExpr(
        "id as chembl_id",
        "synonyms as synonyms",
        "name as pref_name",
        "tradeNames as trade_names",
        "linkedTargets as linkedTargets"
      )
      .withColumn(
        "drug_names",
        array_distinct(flatten(array(col("trade_names"), array(col("pref_name")), col("synonyms"))))
      )
      .withColumn("_drug_name", explode(col("drug_names")))
      .withColumn("drug_name", lower(col("_drug_name")))
      .select("chembl_id", "drug_name", "linkedTargets")
      .distinct()
      .orderBy(col("drug_name"))

    // NOTE - I can probably remove this and leave just the operation on dfChembl?
    drugList
  }
}
