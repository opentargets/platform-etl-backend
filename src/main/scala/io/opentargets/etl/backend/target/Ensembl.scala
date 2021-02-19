package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers
import io.opentargets.etl.backend.spark.Helpers.nest
import org.apache.spark.sql.functions.{col, element_at, size, split}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

case class Ensembl(id: String,
                   biotype: String,
                   approvedName: String,
                   genomicLocation: GenomicLocation,
                   approvedSymbol: String,
)

case class GenomicLocation(chromosome: String, start: Long, end: Long, strand: Integer)

object Ensembl extends LazyLogging {

  def apply(df: DataFrame)(implicit ss: SparkSession): Dataset[Ensembl] = {
    logger.info("Transforming Ensembl inputs.")
    import ss.implicits._
    val ensembl: Dataset[Ensembl] = df
      .filter(col("id").startsWith("ENSG"))
      .select(
        col("id"),
        col("biotype"),
        col("description"),
        col("end").cast(LongType),
        col("start").cast(LongType),
        col("strand").cast(IntegerType),
        col("seq_region_name").as("chromosome"), // chromosome
        col("name").as("approvedSymbol")
      )
      .withColumn("end", col("end").cast(IntegerType))
      .transform(nest(_, List("chromosome", "start", "end", "strand"), "genomicLocation"))
      .transform(descriptionToApprovedName)
      .as[Ensembl]

    ensembl
  }

  /** Return approved name from description */
  private def descriptionToApprovedName(dataFrame: DataFrame): DataFrame = {
    val d = "description"
    dataFrame
      .withColumn(d, split(col(d), "\\[")) // remove redundant source information.
      .withColumn("approvedName", element_at(col(d), 1))
      .drop(d)
  }

}
