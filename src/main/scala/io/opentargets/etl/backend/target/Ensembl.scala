package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Ensembl(id: String,
                   assemblyName: String,
                   biotype: String,
                   description: String,
                   genomicLocation: GenomicLocation,
                   displayName: String,
                   version: Long,
                   ensemblRelease: String)

case class GenomicLocation(chromosome: String, start: Long, end: Long, strand: Long)

object Ensembl extends LazyLogging {

  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Ensembl] = {
    logger.info("Transforming Ensembl inputs.")
    import ss.implicits._
    val ensembl: Dataset[Ensembl] = df
      .transform(Helpers.snakeToLowerCamelSchema)
      .filter(col("isReference") && col("id").startsWith("ENSG"))
      .select("id",
              "assemblyName",
              "biotype",
              "description",
              "end",
              "start",
              "strand",
              "seqRegionName", // chromosome
              "displayName",
              "version",
              "ensemblRelease")
      .withColumnRenamed("seqRegionName", "chromosome")
      .transform(nest(_, List("chromosome", "start", "end", "strand"), "genomicLocation"))
      .as[Ensembl]

    ensembl
  }

}
