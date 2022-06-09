package io.opentargets.etl.backend.genetics

import io.opentargets.etl.backend.Configuration.Genetics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{abs, broadcast, col, when}

object Gene {

  def getGeneDf(dataframe: DataFrame, approvedBioTypes: List[String]): DataFrame = {
    val excludedChromosomes: Set[String] = Set("MT")
    dataframe
      .select(
        col("id") as "gene_id",
        col("genomicLocation.*"),
        col("biotype"),
        when(col("genomicLocation.strand") > 0, col("genomicLocation.start"))
          .otherwise(col("genomicLocation.end")) as "tss"
      )
      .filter(
        (col("biotype") isInCollection approvedBioTypes.toSet) && !(col(
          "chromosome"
        ) isInCollection excludedChromosomes)
      )
      .withColumnRenamed("chromosome", "chr_id")
      .repartitionByRange(col("chr_id"), col("tss"))
      .sortWithinPartitions(col("chr_id"), col("tss"))
  }

  /** @param variant
    *   genetic variant
    * @param distance
    *   absolute difference between variant location and gene transcription start site.
    * @param target
    *   index returned by Gene.getGeneDf
    * @return
    *   variants combined with targets on the same chromosome and within the prescibed distance.
    */
  def variantGeneDistance(variant: DataFrame, distance: Long)(target: DataFrame): DataFrame = {

    variant
      .join(
        broadcast(target.select(col("chr_id"), col("tss"), col("gene_id"))),
        (variant("chr_id") === target("chr_id")) && (abs(
          col("position") - col("tss")
        ) <= distance)
      )
      .drop(target("chr_id"))
      .withColumn("d", abs(col("position") - col("tss")))
  }
}
