package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, struct, trim}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Tep(targetFromSource: String, description: String, disease: String, url: String)

object Tep extends LazyLogging {

  /**
    * @param df tep input file provided by Open Targets data team
    */
  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Tep] = {
    import ss.implicits._
    logger.info("Transforming Tep inputs")

    df.select(
        trim(col("TEP_url")) as "url",
        trim(col("description")) as "description",
        trim(col("disease")) as "disease",
        col("targetFromSource")
      )
      .as[Tep]
  }

}
