package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Ensembl(id: String,
                   assemblyName: String,
                   biotype: String,
                   description: String,
                   end: Long,
                   start: Long,
                   strand: Long,
                   seqRegionName: String,
                   displayName: String,
                   version: Long,
                   ensemblRelease: String)

object Ensembl extends LazyLogging {

  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Ensembl] = {
    logger.info("Transforming Ensembl inputs.")
    import ss.implicits._
    df.transform(Helpers.snakeToLowerCamelSchema)
      .filter(col("isReference") && col("id").startsWith("ENSG"))
      .select("id",
              "assemblyName",
              "biotype",
              "description",
              "end",
              "start",
              "strand",
              "seqRegionName",
              "displayName",
              "version",
              "ensemblRelease")
      .as[Ensembl]
  }

}
