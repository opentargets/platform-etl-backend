package io.opentargets.etl.backend.openfda.stage

import io.opentargets.etl.backend.ETLSessionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, array_distinct, col, concat, explode, lower}

/*
    Project     : io-opentargets-etl-backend
    Timestamp   : 2021-08-04T12:03
    Author      : Manuel Bernal Llinares <mbdebian@gmail.com>
*/

object PrepareAdverseEventData {
  def apply(fdaRawData: DataFrame)(implicit context: ETLSessionContext) = {

    val sparkSession = context.sparkSession
    import sparkSession.implicits._

    val fdasF = fdaRawData
      .withColumn("reaction", explode(col("patient.reaction")))
      // after explode this we will have reaction-drug pairs
      .withColumn("drug", explode(col("patient.drug")))
      // just the fields we want as columns
      .selectExpr(
        "safetyreportid",
        "serious",
        "receivedate",
        "ifnull(seriousnessdeath, '0') as seriousness_death",
        "qualification",
        "trim(translate(lower(reaction.reactionmeddrapt), '^', '\\'')) as reaction_reactionmeddrapt",
        "ifnull(lower(drug.medicinalproduct), '') as drug_medicinalproduct",
        "ifnull(drug.openfda.generic_name, array()) as drug_generic_name_list",
        "ifnull(drug.openfda.brand_name, array()) as drug_brand_name_list",
        "ifnull(drug.openfda.substance_name, array()) as drug_substance_name_list",
        "drug.drugcharacterization as drugcharacterization"
      )
      // we dont need these columns anymore
      .drop("patient", "reaction", "drug", "_reaction", "seriousnessdeath")
      // delicated filter which should be looked at FDA API to double check
      .where(col("qualification")
        .isInCollection(Seq("1", "2", "3")) and col("drugcharacterization") === "1")
      // drug names comes in a large collection of multiple synonyms but it comes spread across multiple fields
      .withColumn(
        "drug_names",
        array_distinct(
          concat(col("drug_brand_name_list"),
            array(col("drug_medicinalproduct")),
            col("drug_generic_name_list"),
            col("drug_substance_name_list")))
      )
      // the final real drug name
      .withColumn("_drug_name", explode(col("drug_names")))
      .withColumn("drug_name", lower(col("_drug_name")))
      // rubbish out
      .drop("drug_generic_name_list", "drug_substance_name_list", "_drug_name")
      .where($"drug_name".isNotNull and $"reaction_reactionmeddrapt".isNotNull and
        $"safetyreportid".isNotNull and $"seriousness_death" === "0" and
        $"drug_name" =!= "")
    fdasF
  }
}
