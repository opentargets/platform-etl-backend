package io.opentargets.etl.backend.targetEngine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, explode_outer, split}

object UniprotLocationFunctions {

  def FindParentChidCousins(uniprotDF: DataFrame): DataFrame = {

    val explodedIsADF = uniprotDF
      .select(
        col("Name"),
        col("Subcellular location ID").as("SubcellID"),
        col("Is part of"),
        explode_outer(split(col("Is a"), ",")) as "Is_a_exploded"
      )

    val explodedAsPartOf = explodedIsADF
      .select(
        col("Name"),
        col("SubcellID"),
        col("Is_a_exploded"),
        explode_outer(split(col("Is part of"), ",")).as("Is_part_exploded")
      )

    val firstDF = explodedAsPartOf
      .select(
        col("Name"),
        col("SubcellID"),
        (split(col("Is_a_exploded"), ";").getItem(0)).as("Is_a_exploded_SL"),
        (split(col("Is_part_exploded"), ";").getItem(0)).as("Is_part_SL")
      )

    val parentalDF = firstDF
      .select(
        col("Name"),
        col("SubcellID"),
        col("Is_a_exploded_SL").as("Is_a")
      )
      .distinct()

    val childDF = firstDF
      .select("SubcellID", "Is_a_exploded_SL")
      .distinct()
      .groupBy("Is_a_exploded_SL")
      .agg(collect_list(col("SubcellID")).as("SubcellID_child"))

    val parentChildDF = parentalDF
      .join(childDF, parentalDF.col("SubcellID") === childDF.col("Is_a_exploded_SL"), "left")
      .select("Name", "SubcellID", "Is_a", "SubcellID_child")

    val cousinsDF = firstDF
      .groupBy(col("Is_part_SL"))
      .agg(collect_list("SubcellID").as("SubcellID_are_part"))

    val parentChildCousinsDF = parentChildDF
      .join(cousinsDF, cousinsDF.col("Is_part_SL") === parentChildDF.col("SubcellID"), "left")
      .select(
        col("Name"),
        col("SubcellID"),
        col("Is_a"),
        col("SubcellID_child").as("Child_SLterms/parent_of"),
        col("SubcellID_are_part").as("Contains_SLterms"),
        concat_ws(",", col("SubcellID"), col("SubcellID_child"), col("SubcellID_are_part"))
          .as("concat")
      )

    val finishedCousinsDF =
      parentChildCousinsDF.select(col("*"), split(col("concat"), ",").as("toSearch"))

    finishedCousinsDF
  }

}
