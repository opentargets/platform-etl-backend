package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TepWithId(ensemblId: String, tep: Tep)
case class Tep(name: String, uri: String)

object Tep extends LazyLogging {

  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[TepWithId] = {
    import ss.implicits._
    logger.info("Transforming Tep inputs")
    df.withColumnRenamed("Ensembl_id", "ensemblId")
      .withColumnRenamed("OT_Target_name", "name")
      .withColumnRenamed("URI", "uri")
      .transform(nest(_, List("name", "uri"), "tep"))
      .as[TepWithId]
  }

}
