package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TepWithId(ensemblId: String, tep: Tep)

case class Tep(description: String, therapeutic_area: String, url: String)

object Tep extends LazyLogging {

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
      .distinct // there are duplicate records in the input file.
      .as[TepWithId]
  }

}
