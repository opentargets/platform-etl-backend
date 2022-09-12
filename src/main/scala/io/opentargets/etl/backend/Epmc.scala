package io.opentargets.etl.backend

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.etl.backend.spark.{IOResource, IoHelpers}
import io.opentargets.etl.backend.spark.IoHelpers.IOResources
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  collect_set,
  length,
  struct,
  sum,
  trim,
  when,
  size,
  lit
}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Epmc extends LazyLogging {

  def apply()(implicit context: ETLSessionContext): IOResources = {
    implicit val ss: SparkSession = context.sparkSession

    logger.info("Prepare EPMC evidence")

    val conf = context.configuration.epmc

    val inputDataFrames = IoHelpers.readFrom(
      Map(
        "cooccurences" -> conf.input.cooccurences
      )
    )

    val cooccurencesDf = compute(
      inputDataFrames("cooccurences").data,
      conf.excludedTargetTerms,
      conf.sectionsOfInterest
    )

    val evidence = cooccurencesDf
      .withColumn("datasourceId", lit("europepmc"))
      .withColumn("datatypeId", lit("literature"))
      .select(
        "datasourceId",
        "datatypeId",
        "targetFromSourceId",
        "diseaseFromSourceMappedId",
        "resourceScore",
        "literature",
        "textMiningSentences",
        "pmcIds"
      )
      .cache()

    logger.info("EPMC disease target evidence saved.")
    if (conf.printMetrics) {
      logger.info(s"Number of evidence: ${cooccurencesDf.count()}")
      logger.info(
        s"Number of publications: ${cooccurencesDf.select(col("publicationIdentifier")).count()}"
      )
      logger.info(
        s"Number of publications without pubmed ID: ${cooccurencesDf
            .filter(col("publicationIdentifier").contains("PMC"))
            .select("publicationIdentifier")
            .distinct
            .count()}"
      )
      logger.info(
        s"Number of targets: ${evidence.select(col("targetFromSourceId")).distinct.count()}"
      )
      logger.info(
        s"Number of diseases: ${evidence.select(col("diseaseFromSourceMappedId")).distinct.count()}"
      )
      logger.info(
        f"Number of associations: ${evidence.select(col("diseaseFromSourceMappedId"), col("targetFromSourceId")).dropDuplicates().count()}"
      )
    }

    logger.info(s"Write EMPC data to ${conf.output.path}")
    val dataframesToSave = Map(
      // coalesce to maintain logic previously used by datateam. A single file is used for metrics calculations.
      "epmc" -> IOResource(evidence.coalesce(1), conf.output)
    )

    IoHelpers.writeTo(dataframesToSave)
  }

  private def compute(
      df: DataFrame,
      excludedTerms: List[String],
      sectionOfInterest: List[String]
  ): DataFrame =
    df.filter(col("section").isin(sectionOfInterest: _*))
      .withColumn("pmid", trim(col("pmid") cast StringType))
      .withColumn("publicationIdentifier", coalesce(col("pmid"), col("pmcid")))
      .filter(
        col("type") === "GP-DS" &&
          col("isMapped") &&
          col("publicationIdentifier").isNotNull &&
          length(col("text")) < 600 && !col("label1").isin(excludedTerms: _*)
      )
      .withColumnRenamed("keywordId1", "targetFromSourceId")
      .withColumnRenamed("keywordId2", "diseaseFromSourceMappedId")
      .groupBy("publicationIdentifier", "targetFromSourceId", "diseaseFromSourceMappedId")
      .agg(
        collect_set(col("pmcid")).alias("pmcIds"),
        collect_set(col("pmid")).alias("literature"),
        collect_set(
          struct(
            col("text"),
            col("start1").alias("tStart"),
            col("end1").alias("tEnd"),
            col("start2").alias("dStart"),
            col("end2").alias("dEnd"),
            col("section")
          )
        ).alias("textMiningSentences"),
        sum(col("evidence_score")).alias("resourceScore")
      )
      .withColumn("pmcIds", when(size(col("pmcIds")) =!= 0, col("pmcIds")))
      .filter(col("resourceScore") > 1)
      .cache()

}
