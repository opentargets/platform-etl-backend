package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Tep(
    targetFromSourceId: String,
    description: String,
    therapeuticArea: String,
    url: String
)

object Tep extends LazyLogging {

  /** @param df tep input file provided by Open Targets data team
    */
  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Tep] = {
    import ss.implicits._
    logger.info("Transforming Tep inputs")

    val tepSchema = Encoders.product[Tep].schema.map { f =>
      f.dataType match {
        case StringType => trim(col(f.name)).as(f.name)
        case _          => col(f.name)
      }
    }

    df.select(tepSchema: _*).as[Tep]
  }

}
