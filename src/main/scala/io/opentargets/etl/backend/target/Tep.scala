package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TepWithId(ensemblId: String, tep: Tep)

case class Tep(description: String, therapeutic_area: String, url: String)

object Tep extends LazyLogging {

  /**
    * @fixme see [discussion](https://app.zenhub.com/workspaces/open-targets-issue-tracker-58a421fd8c85e652659a1486/issues/opentargets/platform/1742)
    *        of dropping duplicates. This is a temporary work-around and in 21.12 we'll update this to collect an array of TEP
    *        objects.
    */
  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[TepWithId] = {
    import ss.implicits._
    logger.info("Transforming Tep inputs")
    df.select(
        col("gene_id") as "ensemblId",
        struct(
          col("description"),
          col("disease") as "therapeutic_area",
          col("TEP_url") as "url"
        ) as "tep"
      )
      .dropDuplicates("ensemblId")
      .as[TepWithId]
  }

}
