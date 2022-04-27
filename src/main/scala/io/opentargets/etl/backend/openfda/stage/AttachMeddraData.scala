package io.opentargets.etl.backend.openfda.stage

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.ETLSessionContext
import io.opentargets.etl.backend.openfda.stage.PrepareForMontecarlo.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lower, regexp_replace, split}

object AttachMeddraData extends LazyLogging {
  def apply(
      fdaData: DataFrame,
      targetDimensionColId: String,
      meddraPreferredTermsData: DataFrame,
      meddraLowLevelTermsData: DataFrame
  )(implicit context: ETLSessionContext) = {

    logger.info(s"Attach Meddra information, target dimension '${targetDimensionColId}'")
    val meddraPreferred = meddraPreferredTermsData
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$+", ","))
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$$", ""))
      .withColumn("_c0", split(col("_c0"), ","))
      .select(
        context.configuration.openfda.meddraPreferredTermsCols.zipWithIndex.map(i =>
          col("_c0").getItem(i._2).as(s"${i._1}")
        ): _*
      )

    val meddraPreferredColsToLower = meddraPreferred.columns.filter(_.contains("name"))
    val dfMeddraPrefferedTerms = meddraPreferredColsToLower.foldLeft(meddraPreferred)((df, c) =>
      df.withColumn(c, lower(col(c)))
    )

    val meddraLowLevel = meddraLowLevelTermsData
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$+", ","))
      .withColumn("_c0", regexp_replace(col("_c0"), "\\$$", ""))
      .withColumn("_c0", split(col("_c0"), ","))
      .select(
        context.configuration.openfda.meddraLowLevelTermsCols.zipWithIndex.map(i =>
          col("_c0").getItem(i._2).as(s"${i._1}")
        ): _*
      )

    val meddraLowLevelColsToLower = meddraLowLevel.columns.filter(_.contains("name"))
    val dfMeddraLowLevelTerms =
      meddraLowLevelColsToLower.foldLeft(meddraLowLevel)((df, c) => df.withColumn(c, lower(col(c))))

    // Add Meddra Preferred Terms
    val fdaMeddraPreferred = fdaData.join(
      dfMeddraPrefferedTerms,
      fdaData("reaction_reactionmeddrapt") === dfMeddraPrefferedTerms("pt_name"),
      "left_outer"
    )
    // Add Meddra Low Level Terms
    val fdaMeddraPreferredAndLowLevel = fdaMeddraPreferred.join(
      dfMeddraLowLevelTerms,
      fdaMeddraPreferred("reaction_reactionmeddrapt") === dfMeddraLowLevelTerms("llt_name"),
      "left_outer"
    )
    // Take best
    fdaMeddraPreferredAndLowLevel
      .withColumn("meddraCode", coalesce(col("pt_code"), col("llt_code")))
      .drop("pt_name", "llt_name", "pt_code", "llt_code")
      .dropDuplicates(Seq(targetDimensionColId, "reaction_reactionmeddrapt"))
  }

}
