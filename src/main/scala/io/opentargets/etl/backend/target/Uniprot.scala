package io.opentargets.etl.backend.target

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.Helpers.nest
import io.opentargets.etl.preprocess.uniprot.UniprotEntryParsed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  * @param uniprotId            current accession number
  * @param synonyms             uniprot recommended and alternative names
  * @param functionDescriptions from uniprot comments
  * @param proteinIds           old accession numbers
  * @param subcellularLocations from uniprot comments
  * @param dbXrefs              references to other libraries.
  */
case class Uniprot(
    uniprotId: String,
    synonyms: Seq[LabelAndSource],
    functionDescriptions: Seq[String],
    proteinIds: Seq[IdAndSource],
    subcellularLocations: Seq[LocationAndSource],
    dbXrefs: Seq[IdAndSource]
)

object Uniprot extends LazyLogging {

  val id = "uniprotId"

  def apply(dfRaw: DataFrame)(implicit ss: SparkSession): Dataset[Uniprot] = {
    logger.info("Processing Uniprot inputs")
    import ss.implicits._
    val uniprotDfWithId = dfRaw
      .as[UniprotEntryParsed]
      .filter(size(col("accessions")) > 0) // null return -1 so remove those too
      .withColumn(id, expr("accessions[0]"))
      .withColumn("synonyms", array_union(col("names"), col("synonyms")))
      .withColumnRenamed("functions", "functionDescriptions")
      .drop("id", "names")

    val dbRefs = handleDbRefs(uniprotDfWithId)
    val synonyms =
      TargetUtils.transformColumnToLabelAndSourceStruct(uniprotDfWithId, id, "synonyms", "uniprot")
    val proteinIds =
      TargetUtils.transformColumnToIdAndSourceStruct(id,
                                                     "accessions",
                                                     "uniprot",
                                                     Some("proteinIds"))(uniprotDfWithId)
    val subcellularLocations =
      TargetUtils.transformColumnToLabelAndSourceStruct(uniprotDfWithId,
                                                        id,
                                                        "locations",
                                                        "uniprot",
                                                        Some("location"),
                                                        Some("subcellularLocations"))

    Seq(uniprotDfWithId.drop("synonyms", "functions", "dbXrefs", "accessions", "locations"),
        dbRefs,
        synonyms,
        proteinIds,
        subcellularLocations).reduce((acc, df) => acc.join(df, Seq(id), "left_outer")).as[Uniprot]
  }

  private def handleDbRefs(dataFrame: DataFrame): DataFrame = {
    val ref = "dbXrefs"
    dataFrame
      .select(col(id), explode(col(ref)).as(ref))
      .withColumn(ref, split(col(ref), " "))
      .withColumn("id", element_at(col(ref), 2))
      .withColumn("source", element_at(col(ref), 1))
      .drop(ref)
      .transform(nest(_, List("id", "source"), ref))
      .groupBy(id)
      .agg(collect_set(ref).as(ref))
  }
}
